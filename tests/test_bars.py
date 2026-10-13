"""Tests for spark/common/bars.py on a local SparkSession"""

from datetime import date, datetime

import pytest

import os

import sys

from pyspark.sql import SparkSession  

from common.bars import ohlcv_bars  

TRADES = "symbol string, price double, size double, timestamp timestamp"


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]").appName("beacon-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def t(minute, second):
    return datetime(2026, 10, 13, 14, minute, second)


def test_open_close_follow_event_time_not_arrival_order(spark):
    # Rows arrive out of event-time order
    trades = spark.createDataFrame([
        ("AAPL", 101.0, 1.0, t(0, 30)),
        ("AAPL", 100.0, 2.0, t(0, 5)),   # earliest -> open
        ("AAPL", 103.0, 1.0, t(0, 50)),  # latest -> close
        ("AAPL", 99.0, 3.0, t(0, 20)),
    ], TRADES)
    bars = ohlcv_bars(trades, "1 minute").collect()
    assert len(bars) == 1
    b = bars[0]
    assert (b.open, b.high, b.low, b.close, b.volume) == (100.0, 103.0, 99.0, 103.0, 7.0)


def test_bars_group_by_symbol_and_window(spark):
    trades = spark.createDataFrame([
        ("AAPL", 100.0, 1.0, t(0, 10)),
        ("AAPL", 105.0, 1.0, t(1, 10)),
        ("TSLA", 200.0, 4.0, t(0, 40)),
    ], TRADES)
    bars = {(r.symbol, r.window_start): r for r in ohlcv_bars(trades, "1 minute").collect()}
    assert set(bars) == {("AAPL", t(0, 0)), ("AAPL", t(1, 0)), ("TSLA", t(0, 0))}
    assert bars[("AAPL", t(1, 0))].window_end == t(2, 0)
    assert bars[("TSLA", t(0, 0))].window_date == date(2026, 10, 13)


def test_five_minute_window_spans_minutes(spark):
    trades = spark.createDataFrame([
        ("AAPL", 100.0, 1.0, t(0, 10)),
        ("AAPL", 104.0, 1.0, t(4, 50)),
    ], TRADES)
    bars = ohlcv_bars(trades, "5 minutes").collect()
    assert len(bars) == 1
    assert (bars[0].open, bars[0].close) == (100.0, 104.0)


def test_output_columns(spark):
    trades = spark.createDataFrame([("AAPL", 100.0, 1.0, t(0, 10))], TRADES)
    assert ohlcv_bars(trades, "1 minute").columns == [
        "symbol", "window_start", "window_end", "window_date", "open", "high", "low", "close", "volume",
    ]