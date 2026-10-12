"""
Delta Lake helper functions for the Spark streaming consumers
"""

from __future__ import annotations

from datetime import date
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, regexp_replace, row_number
from pyspark.sql.window import Window


def sanitize_symbol(symbol: Column) -> Column:
    """'BTC/USD' -> 'BTC-USD'. A '/' breaks partition paths so we replace it with a '-'."""
    return regexp_replace(symbol, "/", "-")


def upsert(spark: SparkSession, df: DataFrame, path: str, keys: list[str], partition_by: list[str],
           range_col: str | None = None) -> None:    
    """
    Idempotent write: MERGE df into the Delta table at path on keys. The table is created on first write.

    Natch condition pins target to partition window so MERGE
    doesnt have to scan every file in the table to find matches (saves batch time)
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy(*partition_by).save(path)
        return

    present = df.select(*partition_by).distinct().collect()
    match = [col(f"t.{k}") == col(f"s.{k}") for k in keys]
    prune = [col(f"t.{c}").isin(sorted({r[c] for r in present})) for c in partition_by]
    if range_col:
        lo, hi = df.agg(spark_min(range_col), spark_max(range_col)).first()
        prune += [col(f"t.{range_col}") >= lo, col(f"t.{range_col}") <= hi]
    condition = reduce(lambda a, b: a & b, match + prune)

    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def recent_bars(spark: SparkSession, path: str, symbols: list[str], since: date, per_symbol: int) -> DataFrame:
    """
    Last per_symbol bars for each symbol reading only (symbol, window_date) partitions on or after since.
    Partition pruning bounds the read by a lookback window not by table size
    """
    w = Window.partitionBy("symbol").orderBy(col("window_start").desc())
    return (
        spark.read.format("delta").load(path)
        .filter(col("symbol").isin(symbols) & (col("window_date") >= since))
        .select("symbol", "window_start", "window_end", "close")
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") <= per_symbol)
        .drop("_rn")
    )