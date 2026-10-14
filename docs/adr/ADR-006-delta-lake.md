# ADR-006: Delta Lake for the Lakehouse

**Date:** decided 2026-09-28, recorded 2026-10-13
**Status:** Accepted

## Context

Recomputing indicators produced duplicate rows and `daily_refresh` needs to read, concatenate and overwrite whole tables. Beacon needs upserts, safe re-runs and compaction for
tables written every 30–60 seconds. We use Delta Lake on top of plain Parquet

## Delta Lake over plain Parquet and Iceberg

- **Plain Parquet:** Rejected. No transactions or MERGE 

- **Iceberg:** Rejected. It needs a catalog service. Delta ships as one Spark package, delta-rs gives access from Airflow, and DuckDB can read tables with it's delta extension

- **Bronze layer:** Kafka is not the long-term record. Broker retention is days, and the local broker has
  no volume at all. Bronze keeps every raw payload permanently so silver can be recomputed when an
  indicator implementation changes. 

## Consequences

- MERGE replaces append-and-hope: 1-min bars and indicators, `daily_refresh`, and scored posts are all
  idempotent on their keys.

- OPTIMIZE and VACUUM are a scheduled obligation (`lakehouse_maintenance`).

- Time travel (`versionAsOf`) is available on every table.

- If we want to use Iceberg we can swap the writers (Spark Iceberg runtime, pyiceberg), stand up a REST catalog, and change the dbt sources from `delta_scan` to `iceberg_scan`.