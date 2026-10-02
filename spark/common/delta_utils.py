"""
Delta Lake helper functions for the Spark streaming consumers
"""

from __future__ import annotations
from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import regexp_replace


def sanitize_symbol(symbol: Column) -> Column:
    """'B
    TC/USD' -> 'BTC-USD'. A '/' breaks partition paths so we replace it with a '-' 
    """
    return regexp_replace(symbol, "/", "-")

def upsert(spark: SparkSession, df: DataFrame, path: str, keys: list[str], partition_by: list[str]) -> None:
    """
    Idempotent write: MERGE df into the Delta table at path on keys. The table is created on first write.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy(*partition_by).save(path)
        return

    condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )