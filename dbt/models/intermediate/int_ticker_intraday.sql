-- joined 1 min bars to indicators 
select
    o.ticker,
    o.asset_class,
    o.bar_start,
    o.open, o.high, o.low, o.close, o.volume,
    i.sma_20, i.sma_50, i.rsi_14,
    i.bb_upper, i.bb_middle, i.bb_lower,
    i.macd, i.macd_signal, i.macd_hist
from {{ ref('stg_ohlcv_1min') }} o
left join {{ ref('stg_indicators') }} i
    on i.symbol = o.symbol
   and i.bar_start = o.bar_start