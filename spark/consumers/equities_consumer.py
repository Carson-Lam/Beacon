"""
Spark Structured Streaming consumer: market.equities -> rolling OHLCV bars

Reads raw trade/quote events off Kafka, keeps trade events only (they're the
ones carrying price + size), and computes 1-min and 5-min OHLCV bars with a
30s watermark for late data. Output: Parquet under /opt/spark-data/ohlcv/equities/.

Run inside the spark-master container:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark-apps/consumers/equities_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, first, last, max as spark_max, min as spark_min, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP = "kafka:29092"   # internal listener
TOPIC = "market.equities"
OUTPUT_BASE = "/opt/spark-data/ohlcv/equities"
CHECKPOINT_BASE = "/opt/spark-data/_checkpoints/equities"

# Superset schema covering both trade and quote payload shapes.
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
    SparkSession.builder
    .appName("beacon-equities-ohlcv")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
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


def make_ohlcv_query(window_duration: str, output_suffix: str):
    bars = (
        parsed.groupBy(
            window(col("timestamp"), window_duration),
            col("symbol"),
        )
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

    return (
        bars.writeStream
        .format("parquet")
        .option("path", f"{OUTPUT_BASE}/{output_suffix}")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{output_suffix}")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


query_1min = make_ohlcv_query("1 minute", "1min")
query_5min = make_ohlcv_query("5 minutes", "5min")

query_1min.awaitTermination()