import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
import requests

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "sentiment.stocktwits"
POLL_INTERVAL_SECONDS = 60

# StockTwits has it's own symbol format
TICKER_TO_STOCKTWITS_SYMBOL = {
    "AAPL": "AAPL",
    "TSLA": "TSLA",
    "SPY": "SPY",
    "BTC": "BTC.X",
    "ETH": "ETH.X",
}

BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Prevent duplicate messages by tracking last seen message ID
last_seen_id = {ticker: None for ticker in TICKER_TO_STOCKTWITS_SYMBOL}


def poll_symbol(ticker: str, st_symbol: str):
    url = BASE_URL.format(symbol=st_symbol)
    try:
        resp = requests.get(url, timeout=10, headers=HEADERS)
    except requests.RequestException as e:
        print(f"[ERROR] {ticker} ({st_symbol}): request failed: {e}")
        return

    if resp.status_code != 200:
        print(f"[ERROR] {ticker} ({st_symbol}): HTTP {resp.status_code}")
        return

    data = resp.json()
    messages = data.get("messages", [])
    if not messages:
        return

    new_messages = [m for m in messages if last_seen_id[ticker] is None or m["id"] > last_seen_id[ticker]]
    new_messages.reverse() # Process oldest messages first 

    for msg in new_messages:
        sentiment = msg.get("entities", {}).get("sentiment")
        sentiment_label = sentiment.get("basic") if sentiment else None

        payload = {
            "ticker": ticker,
            "post_id": msg["id"],
            "text": msg.get("body", ""),
            "source": "stocktwits",
            "timestamp": msg["created_at"],
            "sentiment_label": sentiment_label,
        }
        producer.send(TOPIC, value=payload)
        print(f"[STOCKTWITS] {ticker} <- msg {msg['id']} (sentiment={sentiment_label})")

    if messages:
        last_seen_id[ticker] = max(m["id"] for m in messages)


def run():
    print(f"Starting StockTwits poller for: {list(TICKER_TO_STOCKTWITS_SYMBOL.keys())}")
    while True:
        for ticker, st_symbol in TICKER_TO_STOCKTWITS_SYMBOL.items():
            poll_symbol(ticker, st_symbol)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()