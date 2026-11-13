-- Training target for beacon-direction.
select
    ticker,
    trade_date,
    cast(lead(open, 6) over by_day > lead(open, 1) over by_day as integer) as label
from {{ ref('stg_equities_historical') }}
window by_day as (partition by ticker order by trade_date)