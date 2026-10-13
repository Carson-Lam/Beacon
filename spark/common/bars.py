"""
OHLCV bar construction and the market.* payload contract.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, max_by, min_by, max as spark_max, min as spark_min, sum as spark_sum, to_date, window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

# Payload contract for market.equities and market.crypto
# Trades carry price/size
# Quotes carry bid/ask
MARKET_SCHEMA = StructType([
    StructField("type", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", TimestampType()),
])


def ohlcv_bars(trades: DataFrame, window_duration: str) -> DataFrame:
    """
    OHLCV bars per (symbol, window) from trades with columns symbol, price, size, timestamp
    Open/close are ordered by event time (nondeterministic) 
    """
    return (
        trades.groupBy(window(col("timestamp"), window_duration), col("symbol"))
        .agg(
            min_by("price", "timestamp").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            max_by("price", "timestamp").alias("close"),
            spark_sum("size").alias("volume"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            to_date(col("window.start")).alias("window_date"),
            "open", "high", "low", "close", "volume",
        )
    )