"""
Spark Structured Streaming consumer for market.crypto taking OHLCV bars + technical indicators

Writes to silver:
- 1-min bars and indicators go through foreachBatch with a MERGE on (symbol, window_start)
- 5-min bars use Spark's native Delta streaming sink

"""

import sys
sys.path.append("/opt/spark-apps")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_date, window, first, last, max as spark_max, min as spark_min, sum as spark_sum,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from common.delta_utils import sanitize_symbol, upsert
from indicators.ta_indicators import sma, rsi, bollinger_bands, macd

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "market.crypto"
OHLCV_BASE = "/opt/spark-data/silver/ohlcv/crypto"
FEATURES_BASE = "/opt/spark-data/silver/indicators/crypto"
CHECKPOINT_BASE = "/opt/spark-data/_checkpoints/crypto"

MIN_LOOKBACK_BARS = 60
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
    SparkSession.builder.appName("beacon-crypto-ohlcv")
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
    batch_df.persist()  # reused by count, MERGE and toPandas
    try:
        count = batch_df.count()
        print(f"[1min batch {batch_id}] rows={count}", flush=True)
        if count == 0:
            return

        upsert(spark, batch_df, f"{OHLCV_BASE}/1min", MERGE_KEYS, PARTITION_COLS)

        # Compute indicators per symbol that got a new bar this batch
        new_bars = batch_df.select("symbol", "window_start").toPandas()
        for symbol in new_bars["symbol"].unique():
            new_window_starts = set(new_bars.loc[new_bars["symbol"] == symbol, "window_start"])

            history = (
                spark.read.format("delta").load(f"{OHLCV_BASE}/1min")
                .filter(col("symbol") == symbol)
                .select("window_start", "window_end", "close")
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
            if new_rows.empty:
                continue

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