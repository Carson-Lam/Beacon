select 'intraday' as series, ticker, bar_start as ts, bb_lower, bb_middle, bb_upper
from {{ ref('stg_indicators') }}
where not (bb_lower <= bb_middle and bb_middle <= bb_upper)

union all

select 'daily', ticker, cast(trade_date as timestamptz), bb_lower, bb_middle, bb_upper
from {{ ref('stg_equities_historical') }}
where not (bb_lower <= bb_middle and bb_middle <= bb_upper)