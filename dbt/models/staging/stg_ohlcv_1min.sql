with unioned as (
    {{ union_existing_sources('silver', {'ohlcv_crypto_1min': 'crypto', 'ohlcv_equities_1min': 'equity'}) }}
)

select
    t.ticker,
    u.symbol,
    u.asset_class,
    cast(window_start as timestamptz) as bar_start,
    cast(window_end as timestamptz)   as bar_end,
    cast(open as double)              as open,
    cast(high as double)              as high,
    cast(low as double)               as low,
    cast(close as double)             as close,
    cast(volume as double)            as volume
from unioned u

left join {{ ref('tickers') }} t
    on replace(t.alpaca_symbol, '/', '-') = u.symbol