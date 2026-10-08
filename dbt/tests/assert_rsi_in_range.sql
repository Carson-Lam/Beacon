select 'intraday' as series, ticker, bar_start as ts, rsi_14
from {{ ref('stg_indicators') }}
where rsi_14 < 0 or rsi_14 > 100

union all

select 'daily', ticker, cast(trade_date as timestamptz), rsi_14
from {{ ref('stg_equities_historical') }}
where rsi_14 < 0 or rsi_14 > 100