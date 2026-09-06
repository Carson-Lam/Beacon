import json
import os
import re
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
import praw

load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "beacon-sentiment-producer/0.1")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SUBREDDITS = "wallstreetbets+investing+stocks"
TOPIC = "sentiment.reddit"

# Case sensitive to seperate "SPY" from words like "spy"
# Hardcoded for temporary phase 2 development, will be later driven by user watchlist
TICKER_PATTERNS = {
    "AAPL": re.compile(r'\$?\bAAPL\b'),
    "TSLA": re.compile(r'\$?\bTSLA\b'),
    "SPY":  re.compile(r'\$?\bSPY\b'),
    "BTC":  re.compile(r'\$?\bBTC\b'),
    "ETH":  re.compile(r'\$?\bETH\b'),
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
)
reddit.read_only = True


def matched_tickers(text: str):
    return [ticker for ticker, pattern in TICKER_PATTERNS.items() if pattern.search(text)]


def handle_submission(submission):
    text = f"{submission.title}\n{submission.selftext or ''}".strip()
    tickers = matched_tickers(text)
    if not tickers:
        return

    ts = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc).isoformat()

    # One message per matched ticker 
    for ticker in tickers:
        payload = {
            "ticker": ticker,
            "post_id": submission.id,
            "text": text,
            "subreddit": submission.subreddit.display_name,
            "upvotes": submission.score,
            "timestamp": ts,
        }
        producer.send(TOPIC, value=payload)
        print(f"[REDDIT] {ticker} <- r/{submission.subreddit.display_name} ({submission.id})")


def run():
    subreddit = reddit.subreddit(SUBREDDITS)
    print(f"Starting Reddit stream for: r/{SUBREDDITS.replace('+', ', r/')}")

    for submission in subreddit.stream.submissions(skip_existing=True):
        try:
            handle_submission(submission)
        except Exception as e:
            print(f"[ERROR] failed to process submission {submission.id}: {e}")


if __name__ == "__main__":
    run()