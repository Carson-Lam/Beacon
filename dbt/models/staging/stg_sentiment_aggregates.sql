select
    ticker,
    cast(computed_at as timestamptz)            as computed_at,
    cast(mention_count_24h as integer)          as mention_count_24h,
    cast(bullish_ratio as double)               as bullish_ratio,
    cast(velocity_1h_vs_24h_avg as double)      as velocity_1h_vs_24h
from {{ source('silver', 'sentiment_aggregates') }}