-- Adds day over day fields for watchlist
-- One row per equity per trading day
select
    ticker,
    asset_class,
    trade_date,
    close,
    lag(close) over w                                as prev_close,
    close - lag(close) over w                        as change_abs,
    close / nullif(lag(close) over w, 0) - 1         as change_pct,
    sma_20, sma_50, rsi_14,
    bb_upper, bb_middle, bb_lower,
    macd, macd_signal, macd_hist
from {{ ref('stg_equities_historical') }}
window w as (partition by ticker order by trade_date)