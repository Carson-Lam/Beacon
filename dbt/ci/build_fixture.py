"""
Builds tiny silver-layer Delta tables so `dbt build` can run in CI without streaming.

Rows satisfy the warehouse contract 
Warm-up rows carry NaN indicators so nan_to_null is exercised
Equities streaming tables are deliberately absent to handle a missing sources

CI only because it overwrites data/silver/
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from deltalake import write_deltalake

if os.environ.get("CI") != "true":
    sys.exit("Refusing to run: this overwrites data/silver/ with fixture tables. CI only.")

DATA = Path(__file__).resolve().parents[2] / "data"
NAN = float("nan")
INDICATORS = ["sma_20", "sma_50", "rsi_14", "bb_upper", "bb_middle", "bb_lower", "macd", "macd_signal", "macd_hist"]
WARMUP = {k: NAN for k in INDICATORS}


def ts(day, hh, mm):
    return datetime(2026, 10, day, hh, mm, tzinfo=timezone.utc)


def ind(sma20, sma50, rsi, mid, half_width, macd, signal):
    return {
        "sma_20": sma20, "sma_50": sma50, "rsi_14": rsi,
        "bb_upper": mid + half_width, "bb_middle": mid, "bb_lower": mid - half_width,
        "macd": macd, "macd_signal": signal, "macd_hist": macd - signal,
    }


def write(rel, rows):
    df = pd.DataFrame(rows)
    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].astype("datetime64[us, UTC]")
    write_deltalake(str(DATA / "silver" / rel), df, mode="overwrite")
    print(f"[FIXTURE] silver/{rel}: {len(df)} rows")


def bar(symbol, start, minutes, o, h, low, c, v):
    return {
        "symbol": symbol, "window_start": start, "window_end": start + timedelta(minutes=minutes),
        "window_date": start.date(), "open": o, "high": h, "low": low, "close": c, "volume": v,
    }


one_min = [
    bar("BTC-USD", ts(1, 14, 0), 1, 60000.0, 60050.0, 59950.0, 60020.0, 1.5),
    bar("BTC-USD", ts(1, 14, 1), 1, 60020.0, 60100.0, 60000.0, 60080.0, 0.8),
    bar("BTC-USD", ts(1, 14, 2), 1, 60080.0, 60090.0, 60010.0, 60030.0, 1.1),
    bar("ETH-USD", ts(1, 14, 0), 1, 2400.0, 2405.0, 2398.0, 2402.0, 10.0),
]
write("ohlcv/crypto/1min", one_min)
write("ohlcv/crypto/5min", [bar("BTC-USD", ts(1, 14, 0), 5, 60000.0, 60100.0, 59950.0, 60030.0, 3.4)])

indicator_values = [
    WARMUP,
    ind(60030.0, 60010.0, 55.0, 60030.0, 40.0, 12.0, 9.0),
    ind(60035.0, 60012.0, 48.0, 60035.0, 45.0, 10.0, 9.5),
    WARMUP,
]
write("indicators/crypto", [
    {**{k: b[k] for k in ("symbol", "window_start", "window_end", "window_date", "close")}, **v}
    for b, v in zip(one_min, indicator_values)
])

write("equities_historical", [
    {"symbol": "AAPL", "date": date(2026, 9, 29), "close": 228.0, **WARMUP},
    {"symbol": "AAPL", "date": date(2026, 9, 30), "close": 229.5, **ind(228.4, 226.0, 58.0, 228.4, 3.1, 1.2, 1.0)},
    {"symbol": "AAPL", "date": date(2026, 10, 1), "close": 227.9, **ind(228.6, 226.2, 52.0, 228.6, 3.0, 1.0, 1.05)},
])


def post(ticker, post_id, at, text, pos, neg, neu, label, user_label):
    return {
        "ticker": ticker, "post_id": post_id, "source": "stocktwits", "timestamp": at, "text": text,
        "finbert_positive": pos, "finbert_negative": neg, "finbert_neutral": neu,
        "finbert_label": label, "user_sentiment_label": user_label,
    }


write("sentiment/scored", [
    post("AAPL", "101", ts(1, 13, 0), "AAPL earnings look strong", 0.80, 0.05, 0.15, "positive", "Bullish"),
    post("AAPL", "102", ts(1, 13, 30), "Trimming AAPL before the call", 0.10, 0.70, 0.20, "negative", "Bearish"),
    post("BTC", "103", ts(1, 13, 45), "BTC flat today", 0.10, 0.10, 0.80, "neutral", None),
])

write("sentiment/aggregates", [
    {"ticker": "AAPL", "computed_at": ts(1, 14, 0), "mention_count_24h": 2,
     "bullish_ratio": 0.5, "velocity_1h_vs_24h_avg": 12.0},
    {"ticker": "BTC", "computed_at": ts(1, 14, 0), "mention_count_24h": 1,
     "bullish_ratio": NAN, "velocity_1h_vs_24h_avg": 24.0},
])

# dbt's DuckDB file and external marts need their directories to exist
(DATA / "warehouse").mkdir(parents=True, exist_ok=True)
(DATA / "gold").mkdir(parents=True, exist_ok=True)