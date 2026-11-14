-- A label needs the open 6 trading days ahead 
-- Test Fails on a label where fewer than 6 later bars exist and if the label 
-- where 6 bars exist .
with bars as (
    select
        ticker,
        trade_date,
        count(*) over (
            partition by ticker order by trade_date
            rows between 1 following and unbounded following
        ) as future_bars
    from {{ ref('stg_equities_historical') }}
)

select b.ticker, b.trade_date, b.future_bars, l.label
from bars b
join {{ ref('mart_model_labels') }} l using (ticker, trade_date)
where (b.future_bars < 6 and l.label is not null)
   or (b.future_bars >= 6 and l.label is null)