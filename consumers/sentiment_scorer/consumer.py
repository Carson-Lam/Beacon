import json
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from transformers import pipeline

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPICS = ["sentiment.reddit", "sentiment.stocktwits"]
OUTPUT_TOPIC = "sentiment.scored"
SILVER_SENTIMENT = "data/silver/sentiment"
SCORED_BASE = f"{SILVER_SENTIMENT}/scored"
AGGREGATES_BASE = f"{SILVER_SENTIMENT}/aggregates"
CONSUMER_GROUP = "finbert-scorer"

FLUSH_INTERVAL_SECONDS = 60
ROLLING_WINDOW = timedelta(hours=24)
VELOCITY_RECENT_WINDOW = timedelta(hours=1)

# Loaded once at startup & reused for every message
print("Loading ProsusAI/finbert (CPU)... this can take a bit on first run "
      "while weights download from HuggingFace Hub.")
finbert = pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    tokenizer="ProsusAI/finbert",
    top_k=None,
    device=-1,  # -1 for CPU
)
print("FinBERT loaded.")

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)
consumer.subscribe(INPUT_TOPICS)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

rolling_history = {}
scored_buffer = []

def parse_timestamp(ts_str: str) -> datetime:
    try:
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def score_text(text: str) -> dict:
    """Returns {'positive': p, 'negative': n, 'neutral': u} probabilities."""
    result = finbert(text[:512])[0]  # Finbert max tokenlength
    return {r["label"]: r["score"] for r in result}


def process_message(topic: str, data: dict):
    # 7/8/2026: Reddit is not working, write ghost infra so we can switch if needed
    source = "reddit" if topic == "sentiment.reddit" else "stocktwits"
    ticker = data["ticker"]
    text = data.get("text", "") or ""
    if not text.strip():
        return

    scores = score_text(text)
    finbert_label = max(scores, key=scores.get)
    now = parse_timestamp(data["timestamp"])

    scored = {
        "ticker": ticker,
        "post_id": data.get("post_id"),
        "source": source,
        "timestamp": data["timestamp"],
        "text": text,
        "finbert_positive": scores.get("positive", 0.0),
        "finbert_negative": scores.get("negative", 0.0),
        "finbert_neutral": scores.get("neutral", 0.0),
        "finbert_label": finbert_label,
        "user_sentiment_label": data.get("sentiment_label"),
    }
    producer.send(OUTPUT_TOPIC, value=scored)
    scored_buffer.append(scored)
    print(f"[SCORED] {ticker} ({source}) -> {finbert_label} "
          f"(pos={scores.get('positive', 0):.2f} "
          f"neg={scores.get('negative', 0):.2f} "
          f"neu={scores.get('neutral', 0):.2f})")

    rolling_history.setdefault(ticker, deque())
    rolling_history[ticker].append((now, finbert_label))


def prune_old_entries(ticker: str, reference_time: datetime):
    history = rolling_history.get(ticker)
    if not history:
        return
    cutoff = reference_time - ROLLING_WINDOW
    while history and history[0][0] < cutoff:
        history.popleft()


def compute_aggregates(ticker: str, reference_time: datetime) -> dict | None:
    prune_old_entries(ticker, reference_time)
    history = rolling_history.get(ticker)
    if not history:
        return None

    mention_count_24h = len(history)
    positive = sum(1 for _, label in history if label == "positive")
    negative = sum(1 for _, label in history if label == "negative")
    bullish_ratio = positive / (positive + negative) if (positive + negative) > 0 else None

    recent_cutoff = reference_time - VELOCITY_RECENT_WINDOW
    mentions_last_1h = sum(1 for ts, _ in history if ts >= recent_cutoff)
    avg_hourly_rate_24h = mention_count_24h / 24
    velocity_1h_vs_24h_avg = (
        mentions_last_1h / avg_hourly_rate_24h if avg_hourly_rate_24h > 0 else None
    )

    return {
        "ticker": ticker,
        "computed_at": reference_time.isoformat(),
        "mention_count_24h": mention_count_24h,
        "bullish_ratio": bullish_ratio,
        "velocity_1h_vs_24h_avg": velocity_1h_vs_24h_avg,
    }


def flush_aggregates():
    now = datetime.now(timezone.utc)
    for ticker in list(rolling_history.keys()):
        agg = compute_aggregates(ticker, now)
        if agg is None:
            continue
        row = pd.DataFrame([agg])
        row["computed_at"] = pd.to_datetime(row["computed_at"], utc=True).astype("datetime64[us, UTC]")
        write_deltalake(AGGREGATES_BASE, row, mode="append", partition_by=["ticker"])
        print(f"[AGGREGATE] {ticker}: mentions_24h={agg['mention_count_24h']} "
              f"bullish_ratio={agg['bullish_ratio']} "
              f"velocity={agg['velocity_1h_vs_24h_avg']}")

def flush_scored_posts():
    if not scored_buffer:
        return
    df = pd.DataFrame(scored_buffer)
    df["post_id"] = df["post_id"].astype(str)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601").astype("datetime64[us, UTC]")

    # Kafka auto-commit is at-least-once so a restart can re-score a post. 
    # MERGE on the post identity keeps one row per (source, post_id, ticker).
    try:
        (
            DeltaTable(SCORED_BASE)
            .merge(
                source=df,
                predicate="t.source = s.source AND t.post_id = s.post_id AND t.ticker = s.ticker",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    except TableNotFoundError:
        write_deltalake(SCORED_BASE, df, partition_by=["ticker"])

    print(f"[SCORED FLUSH] merged {len(df)} post(s) into {SCORED_BASE}")
    scored_buffer.clear()

def run():
    print(f"Starting FinBERT scorer, subscribed to: {INPUT_TOPICS}")
    last_flush = time.time()
    while True:
        records = consumer.poll(timeout_ms=5000)
        for topic_partition, messages in records.items():
            for msg in messages:
                try:
                    process_message(msg.topic, msg.value)
                except Exception as e:
                    print(f"[ERROR] failed to process message from {msg.topic}: {e}")

        if time.time() - last_flush >= FLUSH_INTERVAL_SECONDS:
            flush_scored_posts()
            flush_aggregates()
            last_flush = time.time()


if __name__ == "__main__":
    run()