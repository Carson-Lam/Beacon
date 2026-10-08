select '1min' as series, ticker, bar_start as ts
from {{ ref('stg_ohlcv_1min') }}
where bar_start > now() + interval 5 minute

union all

select '5min', ticker, bar_start
from {{ ref('stg_ohlcv_5min') }}
where bar_start > now() + interval 5 minute

union all

select 'daily', ticker, cast(trade_date as timestamptz)
from {{ ref('stg_equities_historical') }}
where trade_date > current_date