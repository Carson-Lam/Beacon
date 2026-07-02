# ADR-001: Kafka as the Streaming Backbone

**Date:** 2026-06-30
**Status:** Accepted

## Context

Beacon requires continuous ingestion of market data from multiple sources (equities, crypto, sentiment). The two viable approaches were REST polling on a timer or a persistent message queue.

## Decision

Use Kafka as the central streaming backbone. All producers publish to Kafka topics and all downstream consumers read from those topics independently.

## Alternatives Considered

- **REST polling**: simpler to set up but couples producers and consumers. Also makes replay impossible (no history)
- **Direct producer-to-consumer**: low latency, but any consumer outage causes data loss.

## Consequences

- Producers and consumers are fully decoupled. Consumers can be added without touching producers
- Messages are replayable, good for debugging.
- Adds overhead, but Docker Compose handles it locally.