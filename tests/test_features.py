"""
Lookahead test for dbt/models/marts/mart_model_features.sql.

Runs the mart's own SQL in an in-memory DuckDB over synthetic history, once on
the full series and once truncated at a date d. Every feature row up to d must
be identical in both runs.
"""

import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from indicators.ta_indicators import indicator_frame

MART_SQL = Path(__file__).resolve().parent.parent / "dbt" / "models" / "marts" / "mart_model_features.sql"
TICKERS = ["AAA", "BBB", "SPY"]
DAYS = pd.bdate_range("2025-01-02", periods=140)
CLOSE_HOUR_NY = 16


def render_mart_sql() -> str:
    sql = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", r"\1", MART_SQL.read_text())
    assert "{{" not in sql and "{%" not in sql, "mart_model_features.sql has unrendered Jinja"
    return sql


def make_bars(seed=11) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for i, ticker in enumerate(TICKERS):
        close = 50.0 * (i + 1) * np.exp(rng.normal(0, 0.015, len(DAYS)).cumsum())
        open_ = close * np.exp(rng.normal(0, 0.005, len(DAYS)))
        frames.append(pd.DataFrame({
            "ticker": ticker,
            "trade_date": DAYS,
            "open": open_,
            "high": np.maximum(open_, close) * (1 + rng.uniform(0, 0.01, len(DAYS))),
            "low": np.minimum(open_, close) * (1 - rng.uniform(0, 0.01, len(DAYS))),
            "close": close,
            "volume": rng.uniform(1e6, 5e6, len(DAYS)),
        }))
    return pd.concat(frames, ignore_index=True)


def make_posts(seed=12) -> pd.DataFrame:
    """Posts at random UTC times, including after the close and on weekends"""
    rng = np.random.default_rng(seed)
    start, end = pd.Timestamp(DAYS[0], tz="UTC"), pd.Timestamp(DAYS[-1], tz="UTC")
    n = 3000
    return pd.DataFrame({
        "ticker": rng.choice(["AAA", "BBB"], n),
        "posted_at": start + (end - start) * rng.uniform(0, 1, n),
        "finbert_label": rng.choice(["positive", "negative", "neutral"], n),
    })


def with_indicators(bars: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, g in bars.groupby("ticker", sort=False):
        g = g.sort_values("trade_date").reset_index(drop=True)
        parts.append(pd.concat([g, indicator_frame(g["close"])], axis=1))
    return pd.concat(parts, ignore_index=True)


def run_mart(bars: pd.DataFrame, posts: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("set TimeZone = 'UTC'")  
    stg_bars = with_indicators(bars)  # noqa: F841
    stg_posts = posts  # noqa: F841
    # trade_date is DATE in stg_equities_historical
    con.execute("create table stg_equities_historical as "
                "select * replace (cast(trade_date as date) as trade_date) from stg_bars")
    con.execute("create table stg_sentiment_scored as select * from stg_posts")
    out = con.execute(render_mart_sql()).df()
    return out.sort_values(["ticker", "trade_date"]).reset_index(drop=True)


def close_of(d: pd.Timestamp) -> pd.Timestamp:
    return d.replace(hour=CLOSE_HOUR_NY).tz_localize("America/New_York").tz_convert("UTC")


@pytest.fixture(scope="module")
def history():
    bars, posts = make_bars(), make_posts()
    return bars, posts, run_mart(bars, posts)


def test_fixture_exercises_every_feature(history):
    _, _, full = history
    assert len(full) > 0
    # Every column must be non-null somewhere
    assert full.notna().any().all(), full.columns[~full.notna().any()].tolist()


# tests four cutoff dates: 
#   deep in the series
#   a Monday (weekend posts)
#   the day before the last
#   the last
@pytest.mark.parametrize("d", [DAYS[70], DAYS[DAYS.day_of_week == 0][-3], DAYS[-2], DAYS[-1]])
def test_features_do_not_read_the_future(history, d):
    bars, posts, full = history
    truncated = run_mart(bars[bars.trade_date <= d], posts[posts.posted_at <= close_of(d)])

    assert set(truncated.loc[truncated.trade_date == d, "ticker"]) == set(TICKERS), \
        "no feature row at d - the comparison would be vacuous"

    expected = full[full.trade_date <= d].reset_index(drop=True)
    # rtol absorbs float summation order in DuckDB windows
    pd.testing.assert_frame_equal(truncated, expected, check_exact=False, rtol=1e-9, atol=1e-12)