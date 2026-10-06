-- Per-ticker daily sentiment from individual scored posts 
-- bullish_ratio uses FinBERT labels and user_bullish_ratio uses user tags

with daily as (
    select
        ticker,
        cast(posted_at as date)                                         as day,
        count(*)                                                        as mention_count,
        count(*) filter (where finbert_label = 'positive')              as positive_count,
        count(*) filter (where finbert_label = 'negative')              as negative_count,
        count(*) filter (where finbert_label = 'neutral')               as neutral_count,
        count(*) filter (where user_sentiment_label = 'bullish')        as user_bullish_count,
        count(*) filter (where user_sentiment_label = 'bearish')        as user_bearish_count
    from {{ ref('stg_sentiment_scored') }}
    group by 1, 2
)

select
    ticker,
    day,
    mention_count,
    positive_count,
    negative_count,
    neutral_count,
    cast(positive_count as double) / nullif(positive_count + negative_count, 0)              as bullish_ratio,
    cast(user_bullish_count as double) / nullif(user_bullish_count + user_bearish_count, 0)  as user_bullish_ratio,
    mention_count / nullif(avg(mention_count) over (
        partition by ticker order by day
        range between interval 7 days preceding and interval 1 day preceding
    ), 0)                                                                                    as velocity_vs_7d_avg
from daily