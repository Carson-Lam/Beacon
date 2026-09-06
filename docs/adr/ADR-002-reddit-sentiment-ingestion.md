# ADR-002: Reddit Sentiment Ingestion

**Date:** 2026-08-31
**Status:** Accepted

## Context

Beacon needs raw social sentiment text feeding the (future) FinBERT scoring
stage. Reddit is the first source; StockTwits follows the same pattern next.

## Decision

- **Sentiment.raw repurposing:** New Kafka topic `sentiment.reddit`, separate from the already-provisioned
  `sentiment.raw`. Each raw social source gets its own topic
  (`sentiment.reddit`, `sentiment.stocktwits`, ...). `sentiment.raw` 
  is left unused for now, can be used as a merged/normalized topic once FinBERT scoring exists.
- **Ticker filtering:** Ticker filtering uses case-sensitive regex matching against post title +
  body, not case-insensitive. This is to prevent false positives with ordinary english words. 
  `$TICKER` cashtag form is also accepted.
- **Multi ticker posts:** One Kafka message is produced per matched ticker per post.
  Multi-ticker post fans out into multiple single-ticker records to 
  keep the downstream schema `{ticker, post_id, text, subreddit, upvotes,
  timestamp}` uniform for the FinBERT consumer.
- Reddit API credentials (`REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`,
  `REDDIT_USER_AGENT`) are stored in `.env`.

## Alternatives Considered

- **Single `sentiment.raw` topic with a `source` field**: Couples different sources, too convoluted

## Consequences

- `kafka-init` needs a corresponding topic-creation line for
  `sentiment.reddit` (added in this change).
- The StockTwits producer (next step) should follow the same
  one-topic-per-source and case-sensitive-ticker conventions.