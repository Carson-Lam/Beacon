-- Ticker Detail chart series 
with intraday as (
    select
        ticker, asset_class, '1m' as timeframe, 
        bar_start as ts, open, high, low, close, volume, sma_20, sma_50, rsi_14, bb_upper, bb_middle, bb_lower, macd, macd_signal, macd_hist
    from {{ ref('int_ticker_intraday') }}
),

daily as (
    select
        ticker, asset_class, '1d' as timeframe, 
        cast(trade_date as timestamptz) as ts,
        cast(null as double) as open, 
        cast(null as double) as high,
        cast(null as double) as low, close, 
        cast(null as double) as volume, sma_20, sma_50, rsi_14, bb_upper, bb_middle, bb_lower, macd, macd_signal, macd_hist
    from {{ ref('int_ticker_daily') }}
)

select * from intraday
union all by name
select * from daily