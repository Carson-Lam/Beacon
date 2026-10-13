"""
Spark Structured Streaming consumer for the market

One module for both asset classes:
    spark-submit ... ohlcv_consumer.py crypto
    spark-submit ... ohlcv_consumer.py equities

Writes to silver:
- 1-min bars and indicators: foreachBatch with a pruned MERGE on (symbol, window_date, window_start)
- 5-min bars: Spark's native Delta streaming sink 
"""

import sys
sys.path.append("/opt/spark-apps")

from datetime import timedelta

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from common.delta_utils import recent_bars, sanitize_symbol, upsert
from common.bars import MARKET_SCHEMA, ohlcv_bars
from indicators.ta_indicators import indicator_frame

if len(sys.argv) != 2 or sys.argv[1] not in ("crypto", "equities"):
    sys.exit("usage: ohlcv_consumer.py crypto|equities")
ASSET = sys.argv[1]

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = f"market.{ASSET}"
OHLCV_BASE = f"/opt/spark-data/silver/ohlcv/{ASSET}"
FEATURES_BASE = f"/opt/spark-data/silver/indicators/{ASSET}"
CHECKPOINT_BASE = f"/opt/spark-data/_checkpoints/{ASSET}"

LOOKBACK_DAYS = 7    
HISTORY_BARS = 200   # well past SMA-50 and MACD warm-up
PARTITION_COLS = ["symbol", "window_date"]
MERGE_KEYS = ["symbol", "window_date", "window_start"]

spark = (
    SparkSession.builder.appName(f"beacon-{ASSET}-ohlcv")
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
    .select(from_json(col("json_str"), MARKET_SCHEMA).alias("data"))
    .select("data.*")
    .filter(col("type") == "trade")
    .withColumn("symbol", sanitize_symbol(col("symbol")))
    .withWatermark("timestamp", "30 seconds")
)

def process_1min_batch(batch_df, batch_id):
    batch_df.persist() 
    try:
        count = batch_df.count()
        print(f"[{ASSET} 1min batch {batch_id}] rows={count}", flush=True)
        if count == 0:
            return

        upsert(spark, batch_df, f"{OHLCV_BASE}/1min", MERGE_KEYS, PARTITION_COLS, range_col="window_start")

        # One partition-pruned read for every symbol in the batch
        new_bars = batch_df.select("symbol", "window_start").toPandas()
        since = new_bars["window_start"].min().date() - timedelta(days=LOOKBACK_DAYS)
        symbols = sorted(new_bars["symbol"].unique().tolist())
        history = recent_bars(spark, f"{OHLCV_BASE}/1min", symbols, since, HISTORY_BARS).toPandas()

        frames = []
        for symbol, h in history.groupby("symbol"):
            h = h.sort_values("window_start").reset_index(drop=True)
            features = pd.concat(
                [h[["symbol", "window_start", "window_end", "close"]], indicator_frame(h["close"])],
                axis=1,
            )
            new_starts = set(new_bars.loc[new_bars["symbol"] == symbol, "window_start"])
            frames.append(features[features["window_start"].isin(new_starts)])

        if not frames:
            return
        new_rows = pd.concat(frames, ignore_index=True)
        if new_rows.empty:
            return

        # One MERGE for all symbols 
        new_rows_df = spark.createDataFrame(new_rows).withColumn("window_date", to_date(col("window_start")))
        upsert(spark, new_rows_df, FEATURES_BASE, MERGE_KEYS, PARTITION_COLS, range_col="window_start")
    finally:
        batch_df.unpersist()


query_1min = (
    ohlcv_bars(parsed, "1 minute").writeStream
    .foreachBatch(process_1min_batch)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/1min")
    .trigger(processingTime="30 seconds")
    .start()
)

query_5min = (
    ohlcv_bars(parsed, "5 minutes").writeStream
    .format("delta")
    .partitionBy(*PARTITION_COLS)
    .option("path", f"{OHLCV_BASE}/5min")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/5min")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

spark.streams.awaitAnyTermination()