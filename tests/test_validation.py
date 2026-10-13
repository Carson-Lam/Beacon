"""Daily ingest unit tests for airflow/dags/validation.py """

import pandas as pd
import pytest

from validation import validate_bars


def bars(**overrides):
    row = {"Date": "2026-10-12", "Open": 228.0, "High": 230.0, "Low": 227.0, "Close": 229.0, "Volume": 5_000_000}
    row.update(overrides)
    return pd.DataFrame([row])


def test_clean_bar_passes():
    validate_bars("AAPL", bars(), prior_close=227.5)


def test_new_ticker_without_history_passes():
    validate_bars("AAPL", bars(), prior_close=None)


def test_normal_move_passes():
    validate_bars("AAPL", bars(Open=200.0, Low=195.0, Close=200.0), prior_close=228.0)  # about -12%


@pytest.mark.parametrize("overrides, message", [
    ({"Volume": 0}, "non-positive volume"),
    ({"Close": None}, "null"),
    ({"Low": 229.5}, "incoherent"),        # low above the close
    ({"High": 228.5}, "incoherent"),       # high below the close
    ({"Open": -1.0, "Low": -1.0}, "non-positive price"),
])
def test_bad_bar_raises(overrides, message):
    with pytest.raises(ValueError, match=message):
        validate_bars("AAPL", bars(**overrides), prior_close=228.0)


def test_missing_column_raises():
    with pytest.raises(ValueError, match="missing columns"):
        validate_bars("AAPL", bars().drop(columns=["Volume"]), prior_close=228.0)


def test_duplicate_dates_raise():
    df = pd.concat([bars(), bars()], ignore_index=True)
    with pytest.raises(ValueError, match="duplicated or out of order"):
        validate_bars("AAPL", df, prior_close=228.0)


def test_split_like_drop_raises_with_backfill_hint():
    # A 4:1 split on a mismatched price basis: 57 vs a stored 228
    with pytest.raises(ValueError, match="historical_backfill"):
        validate_bars("AAPL", bars(Open=57.0, High=58.0, Low=56.5, Close=57.0), prior_close=228.0)