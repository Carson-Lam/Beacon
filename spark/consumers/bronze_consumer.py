"""
Append raw kafka messages to a bronze table

Payload kept as raw JSON string. Kafka metadata makaes every row traceable to original source message

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

KAFKA_BOOTSTRAP = "kafka:29092"
TOPICS = "market.equities,market.crypto,sentiment.reddit,sentiment.stocktwits"
BRONZE_PATH = "/opt/spark-data/bronze/kafka"
CHECKPOINT = "/opt/spark-data/_checkpoints/bronze"

spark = (
    SparkSession.builder.appName("beacon-bronze-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "earliest")  
    .load()
)

bronze = raw.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("value").cast("string").alias("payload"),
    to_date(col("timestamp")).alias("ingest_date"),
)

query = (
    bronze.writeStream
    .format("delta")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("topic", "ingest_date")
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()