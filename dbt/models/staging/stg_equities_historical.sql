select
    symbol                            as ticker,
    symbol,
    'equity'                          as asset_class,
    cast("date" as date)              as trade_date,
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
from {{ source('silver', 'equities_historical') }}