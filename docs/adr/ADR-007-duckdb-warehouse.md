# ADR-007: DuckDB + dbt as the Warehouse

**Date:** decided 2026-09-28, recorded 2026-10-13
**Status:** Accepted

## Context

Beacon needs tested documented data marts with lineage over the lakehouse at $0.

## DuckDB over Postgres and Snowflake

- **DuckDB + dbt-core:** queries the silver Delta tables in place with `delta_scan()`. Marts are written as Parquet to `data/gold/`.
- **Postgres:** Rejected. Every row copied into a server that adds nothing at this data volume.
- **Snowflake:** Rejected. The free trial expires. 

## Consequences
- Models are plain SQL, so switching warehouses means a new `profiles.yml` target and source definitions.

- The DuckDB file allows one writer at a time so `dbt_transform` DAG and manual runs in `beacon-dbt`
  must not overlap.

- There's no query server. The dashboard and MCP server read the gold Parquet marts via FastAPI.