"""
Ingest checks for the daily yfinance pull called by daily_refresh.validate_data.

Uses pandas so tests run it directly 

"""

import pandas as pd

PRICE_COLS = ["Open", "High", "Low", "Close"]
REQUIRED_COLS = ["Date", *PRICE_COLS, "Volume"]

# Largest close-to-close move accepted either direction
# Catches 2:1 and larger splits
MAX_DAILY_MOVE = 1.5


def validate_bars(ticker: str, df: pd.DataFrame, prior_close: float | None) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{ticker}: missing columns {missing}")
    if df.empty:
        raise ValueError(f"{ticker}: no rows")

    nulls = int(df[REQUIRED_COLS].isnull().sum().sum())
    if nulls:
        raise ValueError(f"{ticker}: {nulls} null values in OHLCV columns")

    if (df["Volume"] <= 0).any():
        raise ValueError(f"{ticker}: non-positive volume")
    if (df[PRICE_COLS] <= 0).any().any():
        raise ValueError(f"{ticker}: non-positive price")

    body_low = df[["Open", "Close"]].min(axis=1)
    body_high = df[["Open", "Close"]].max(axis=1)
    if ((df["Low"] > body_low) | (body_high > df["High"])).any():
        raise ValueError(f"{ticker}: incoherent bar (low/high do not bound open/close)")

    dates = pd.to_datetime(df["Date"])
    if dates.duplicated().any() or not dates.is_monotonic_increasing:
        raise ValueError(f"{ticker}: dates are duplicated or out of order")

    closes = df["Close"].astype(float).tolist()
    if prior_close is not None:
        closes = [float(prior_close)] + closes
    for prev, cur in zip(closes, closes[1:]):
        ratio = cur / prev
        if not (1 / MAX_DAILY_MOVE <= ratio <= MAX_DAILY_MOVE):
            raise ValueError(
                f"{ticker}: close {cur:.2f} is {ratio:.2f}x the previous close {prev:.2f}. Likely a split or other "
                f"corporate action, so stored history is on a different price basis. Re-run historical_backfill "
                f"to re-adjust history, then re-run this date."
            )