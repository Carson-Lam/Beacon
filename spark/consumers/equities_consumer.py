"""
Spark Structured Streaming consumer for market.equities taking OHLCV bars + technical indicators

1-min bars and indicators are computed in a single streaming query via
foreachBatch

5-min bars are written separately with no indicators using Spark's native Parquet sink, same as before.

Known limitation: Parquet appends inside foreachBatch are not idempotent under batch replay.

Run inside the spark-master container:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark-apps/consumers/equities_consumer.py
"""

import sys
sys.path.append("/opt/spark-apps")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, first, last, max as spark_max, min as spark_min, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from indicators.ta_indicators import sma, rsi, bollinger_bands, macd

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "market.equities"
OHLCV_BASE = "/opt/spark-data/ohlcv/equities"
FEATURES_BASE = "/opt/spark-data/features/equities"
CHECKPOINT_BASE = "/opt/spark-data/_checkpoints/equities"

MIN_LOOKBACK_BARS = 60  

schema = StructType([
    StructField("type", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", TimestampType()),
])

spark = SparkSession.builder.appName("beacon-equities-ohlcv").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .filter(col("type") == "trade")
    .withWatermark("timestamp", "30 seconds")
)


def make_ohlcv(window_duration: str):
    return (
        parsed.groupBy(window(col("timestamp"), window_duration), col("symbol"))
        .agg(
            first("price").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price").alias("close"),
            spark_sum("size").alias("volume"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "open", "high", "low", "close", "volume",
        )
    )


def process_1min_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Write the new OHLCV bars first 
    batch_df.write.mode("append").parquet(f"{OHLCV_BASE}/1min")

    # Compute indicators per symbol that got a new bar this batch
    new_bars = batch_df.toPandas()
    for symbol in new_bars["symbol"].unique():
        new_window_starts = set(new_bars.loc[new_bars["symbol"] == symbol, "window_start"])

        history = (
            spark.read.parquet(f"{OHLCV_BASE}/1min")
            .filter(f"symbol = '{symbol}'")
            .toPandas()
            .sort_values("window_start")
            .tail(max(MIN_LOOKBACK_BARS, 200))
            .reset_index(drop=True)
        )
        if history.empty:
            continue

        close = history["close"]
        bb_upper, bb_middle, bb_lower = bollinger_bands(close, 20, 2)
        macd_line, macd_signal, macd_hist = macd(close, 12, 26, 9)

        features = pd.DataFrame({
            "symbol": symbol,
            "window_start": history["window_start"],
            "window_end": history["window_end"],
            "close": close,
            "sma_20": sma(close, 20),
            "sma_50": sma(close, 50),
            "rsi_14": rsi(close, 14),
            "bb_upper": bb_upper,
            "bb_middle": bb_middle,
            "bb_lower": bb_lower,
            "macd": macd_line,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
        })

        new_rows = features[features["window_start"].isin(new_window_starts)]
        if not new_rows.empty:
            spark.createDataFrame(new_rows).write.mode("append").parquet(
                f"{FEATURES_BASE}/symbol={symbol}"
            )


query_1min = (
    make_ohlcv("1 minute").writeStream
    .foreachBatch(process_1min_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/1min")
    .trigger(processingTime="30 seconds")
    .start()
)

query_5min = (
    make_ohlcv("5 minutes").writeStream
    .format("parquet")
    .option("path", f"{OHLCV_BASE}/5min")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/5min")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

query_1min.awaitTermination()