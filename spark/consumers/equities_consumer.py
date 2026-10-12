"""
Spark Structured Streaming consumer for market.equities: OHLCV bars + technical indicators.

Mirrors spark/consumers/crypto_consumer.py; 
"""

import sys
sys.path.append("/opt/spark-apps")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, window, first, last, max as spark_max, min as spark_min, sum as spark_sum,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from datetime import timedelta
from common.delta_utils import recent_bars, sanitize_symbol, upsert
from indicators.ta_indicators import sma, rsi, bollinger_bands, macd

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "market.equities"
OHLCV_BASE = "/opt/spark-data/silver/ohlcv/equities"
FEATURES_BASE = "/opt/spark-data/silver/indicators/equities"
CHECKPOINT_BASE = "/opt/spark-data/_checkpoints/equities"

LOOKBACK_DAYS = 7  
HISTORY_BARS = 200 
PARTITION_COLS = ["symbol", "window_date"]
MERGE_KEYS = ["symbol", "window_date", "window_start"]

schema = StructType([
    StructField("type", StringType()),
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("timestamp", TimestampType()),
])

spark = (
    SparkSession.builder.appName("beacon-equities-ohlcv")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
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
    .withColumn("symbol", sanitize_symbol(col("symbol")))
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
            to_date(col("window.start")).alias("window_date"),
            "open", "high", "low", "close", "volume",
        )
    )


def process_1min_batch(batch_df, batch_id):
    batch_df.persist() 
    try:
        count = batch_df.count()
        print(f"[1min batch {batch_id}] rows={count}", flush=True)
        if count == 0:
            return

        upsert(spark, new_rows_df, FEATURES_BASE, MERGE_KEYS, PARTITION_COLS, range_col="window_start")
        # partition-pruned read for every symbol in the batch
        new_bars = batch_df.select("symbol", "window_start").toPandas()
        since = new_bars["window_start"].min().date() - timedelta(days=LOOKBACK_DAYS)
        symbols = sorted(new_bars["symbol"].unique().tolist())
        history = recent_bars(spark, f"{OHLCV_BASE}/1min", symbols, since, HISTORY_BARS).toPandas()

        frames = []
        for symbol, h in history.groupby("symbol"):
            h = h.sort_values("window_start").reset_index(drop=True)
            close = h["close"]
            bb_upper, bb_middle, bb_lower = bollinger_bands(close, 20, 2)
            macd_line, macd_signal, macd_hist = macd(close, 12, 26, 9)

            features = pd.DataFrame({
                "symbol": symbol,
                "window_start": h["window_start"],
                "window_end": h["window_end"],
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

            new_starts = set(new_bars.loc[new_bars["symbol"] == symbol, "window_start"])
            frames.append(features[features["window_start"].isin(new_starts)])

        if not frames:
            return
        new_rows = pd.concat(frames, ignore_index=True)
        if new_rows.empty:
            return

        # One MERGE for all symbols
        new_rows_df = spark.createDataFrame(new_rows).withColumn("window_date", to_date(col("window_start")))
        upsert(spark, new_rows_df, FEATURES_BASE, MERGE_KEYS, PARTITION_COLS)
    finally:
        batch_df.unpersist()


query_1min = (
    make_ohlcv("1 minute").writeStream
    .foreachBatch(process_1min_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/1min")
    .trigger(processingTime="30 seconds")
    .start()
)

query_5min = (
    make_ohlcv("5 minutes").writeStream
    .format("delta")
    .partitionBy(*PARTITION_COLS)
    .option("path", f"{OHLCV_BASE}/5min")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/5min")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

spark.streams.awaitAnyTermination()