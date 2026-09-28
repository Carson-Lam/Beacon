# ADR-004: Spark Structured Streaming vs. Flink

**Date:** 2026-09-28
**Status:** Accepted

## Context

Beacon needs a stream processor for windowed OHLCV bars (1-min and 5-min)
and technical indicators over the Kafka market topics with a watermark
for late-arriving trades. Spark Structured Streaming and Apache Flink are
the two main options.

## Spark over Flink

- **Existing ecosystem:** the indicator code is pandas-based and is reused
  in the Airflow DAGs. PyFlink is less mature for pandas interop.
- **Simpler for Docker:** one custom `apache/spark:3.5.1` image with a
  master and a worker. Flink needs a JobManager, TaskManager, and state
  backend configuration.
- **Latency:** bars run on 30-second micro-batch triggers. Spark has slightly higher latency
  compared to flint, which is fine.

## Consequences

- Streaming latency is bounded by the trigger interval (30s). 
- Not every consumer is Spark. The FinBERT scorer is a plain Python
  consumer because it is one message per model call with no windowed
  aggregation.