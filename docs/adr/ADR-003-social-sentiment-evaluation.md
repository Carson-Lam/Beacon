# ADR-003: Social Sentiment Evaluation

**Date:** 2026-09-27
**Status:** Accepted (Reddit ingestion currently blocked)

## Context

Beacon needs raw social sentiment text feeding the FinBERT scoring stage.
Two social sources were planned: Reddit (via PRAW) and StockTwits (via its
public REST API).

## Reddit API Access Blocked (post-2025 policy change)

Reddit closed self-service developer app creation in November 2025 and
requires new API access requests to go through a manual approval
process. This was discovered mid-build. An access request was submitted but rejected.

**Alternatives considered for this specific blocker:**

- *Static/historical Reddit dataset* 
- *Proceed with StockTwits as the only live social source, keep the
  Reddit producer code as pending*: **chosen**. 

## FinBERT over VADER for sentiment scoring

**VADER:** lexicon-based sentiment tool tuned for general social-media text. FinBERT is a BERT model tuned on financial text with a higher compute cost (~100–300ms/message on CPU) versus VADER's near-instant. Beacon has less messages (low hundreds per hour across tracked tickers), so a bit more latency is acceptable for more accuracy.

**Metric derivation:** the rolling `bullish_ratio` aggregate is computed
from FinBERT's classification not StockTwits' user-declared tag for accuracy. Same method was intended for reddit posts. The user-declared label is still passed through as `user_sentiment_label` in the scored output for comparison against FinBERT's interpretation. This matters for legal framing.

## Consequences

- `sentiment.reddit` and `sentiment.stocktwits` are separate Kafka
  topics so Reddit can be activated later with zero changes to the scoring consumer.

- The FinBERT scorer's rolling-window state is in-memory only so restart silently resets
  `mention_count_24h`, `bullish_ratio`, and `velocity` to zero rather
  than erroring. This a local dev design decision. Possibly pivot to DuckDB for production.

- Sentiment coverage is currently Twitter/X-only.