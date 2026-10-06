-- One row per ticker for the Watchlist: 
-- latest price, latest indicator set, change vs previous close, latest sentiment aggregate
-- Price chooses between latest 1 min bar/latest daily close

with latest_intraday as (
    select * from {{ ref('int_ticker_intraday') }}
    qualify row_number() over (partition by ticker order by bar_start desc) = 1
),

latest_daily as (
    select * from {{ ref('int_ticker_daily') }}
    qualify row_number() over (partition by ticker order by trade_date desc) = 1
),

latest_sentiment as (
    select * from {{ ref('stg_sentiment_aggregates') }}
    qualify row_number() over (partition by ticker order by computed_at desc) = 1
),

candidates as (
    select
        ticker, asset_class, '1m' as timeframe,
        bar_start as price_as_of, 
        bar_start as recency, close, sma_20, sma_50, rsi_14, bb_upper, bb_middle, bb_lower, macd, macd_signal, macd_hist
    from latest_intraday
    union all
    select
        ticker, asset_class, '1d' as timeframe,
        cast(trade_date as timestamptz) as price_as_of,
        cast(trade_date + interval 1 day as timestamptz) as recency,
        close, sma_20, sma_50, rsi_14, bb_upper, bb_middle, bb_lower, macd, macd_signal, macd_hist
    from latest_daily
),

picked as (
    select * from candidates
    qualify row_number() over (partition by ticker order by recency desc) = 1
)

select
    p.ticker, p.asset_class, p.close as price,
    p.price_as_of, p.timeframe as indicators_timeframe,
    case 
        when p.timeframe = '1m' 
        then d.close 
        else d.prev_close 
    end as ref_close,
    p.close - case 
        when p.timeframe = '1m' 
        then d.close 
        else d.prev_close 
    end as change_abs,
    p.close / nullif(
        case 
            when p.timeframe = '1m' 
            then d.close 
            else d.prev_close 
        end, 0
    ) - 1 as change_pct,
    p.sma_20, p.sma_50, p.rsi_14,
    p.bb_upper, p.bb_middle, p.bb_lower,
    p.macd, p.macd_signal, p.macd_hist,
    s.mention_count_24h,
    s.bullish_ratio,
    s.velocity_1h_vs_24h,
    s.computed_at as sentiment_as_of
from picked p
left join latest_daily d on d.ticker = p.ticker
left join latest_sentiment s on s.ticker = p.ticker