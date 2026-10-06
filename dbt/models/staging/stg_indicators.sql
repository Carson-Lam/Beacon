with unioned as (
    {{ union_existing_sources('silver', {'indicators_crypto': 'crypto', 'indicators_equities': 'equity'}) }}
)

select
    split_part(symbol, '-', 1)        as ticker,
    symbol,
    asset_class,
    cast(window_start as timestamptz) as bar_start,
    cast(window_end as timestamptz)   as bar_end,
    cast(close as double)             as close,
    {{ nan_to_null('sma_20') }}       as sma_20,
    {{ nan_to_null('sma_50') }}       as sma_50,
    {{ nan_to_null('rsi_14') }}       as rsi_14,
    {{ nan_to_null('bb_upper') }}     as bb_upper,
    {{ nan_to_null('bb_middle') }}    as bb_middle,
    {{ nan_to_null('bb_lower') }}     as bb_lower,
    {{ nan_to_null('macd') }}         as macd,
    {{ nan_to_null('macd_signal') }}  as macd_signal,
    {{ nan_to_null('macd_hist') }}    as macd_hist
from unioned