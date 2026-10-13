# ADR-005: Great Expectations Considered and Declined

**Date:** 2026-10-13
**Status:** Accepted

## Context

Phase 3 planned a Great Expectations checkpoint on the daily yfinance pull in `daily_refresh`, the one
batch source that feeds the historical feature store and the Phase 4 model.

## Plain pandas checks over Great Expectations

- **Warehouse already covered:** dbt generic and singular tests check grain, RSI range, Bollinger ordering and OHLC coherence on every `dbt build`.

- **Small Ingest checks:** columns, nulls, positive volume and prices, OHLC coherence, ordered unique
  dates, and close vs. the stored prior close. This is about 40 lines of pandas in `airflow/dags/validation.py` unit tested in `tests/test_validation.py`.
- **GX's value doesn't apply yet:** reusable suites and data docs across many sources. 

## Consequences

- The price-sanity check rejects any close more/less than 1.5x the previous close tuned
  against the backfilled history. It catches 2:1 and larger splits, the real yfinance failure mode when
  stored history and a new pull are on different adjustment bases, and the error says to re-run
  `historical_backfill`.

- No data-docs site for ingest checks: a failure shows as a failed `validate_data` task with the reason
  in its log.
  
- Revisit if Beacon adds several batch sources that would share expectation suites.