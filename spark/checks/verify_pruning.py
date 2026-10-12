"""
Phase 3.5 step 4 check: confirms the 1-min consumer's MERGE and history read are partition-pruned.

Prints total active files vs. files in scope for each operation, plus timing metrics
from the table's own MERGE history. Safe to run while the consumers are running (read-only).
"""

import sys
sys.path.append("/opt/spark-apps")

from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from common.delta_utils import recent_bars

TABLE = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/silver/ohlcv/crypto/1min"
SYMBOLS = ["BTC-USD", "ETH-USD"]
LOOKBACK_DAYS = 7
HISTORY_BARS = 200

spark = (
    SparkSession.builder.appName("beacon-verify-pruning")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

today = datetime.now(timezone.utc).date()
table = spark.read.format("delta").load(TABLE)
dates = [r[0] for r in table.select("window_date").distinct().orderBy("window_date").collect()]

# inputFiles() returns the files Spark would actually open after partition pruning
total_files = len(table.inputFiles())
latest = table.agg(spark_max("window_start")).first()[0]
merge_files = len(
    table.filter(
        col("symbol").isin(SYMBOLS)
        & col("window_date").isin([latest.date()])
        & (col("window_start") >= latest - timedelta(minutes=2))
    ).inputFiles()
)
history_files = len(recent_bars(spark, TABLE, SYMBOLS, today - timedelta(days=LOOKBACK_DAYS), HISTORY_BARS).inputFiles())

print(f"\nTable:          {TABLE}")
print(f"Active files:   {total_files} across {len(dates)} dates ({dates[0]} .. {dates[-1]})")
print(f"MERGE scope:    {merge_files} files (batch's partitions + window_start range)")
print(f"History read:   {history_files} files (last {LOOKBACK_DAYS} days)\n")

print("Recent MERGE commits (newest first):")
(
    DeltaTable.forPath(spark, TABLE).history(100)
    .filter(col("operation") == "MERGE")
    .select(
        "version",
        "timestamp",
        col("operationMetrics.numSourceRows").alias("src_rows"),
        col("operationMetrics.numTargetFilesRemoved").alias("files_rewritten"),
        col("operationMetrics.scanTimeMs").alias("scan_ms"),
        col("operationMetrics.executionTimeMs").alias("total_ms"),
    )
    .show(30, truncate=False)
)