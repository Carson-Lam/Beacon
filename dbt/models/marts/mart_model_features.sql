-- Feature rules:
--   * a feature at trade_date d may only read bars dated <= d 
--   * no labels: they live in mart_model_labels
--   * windows count trading days (rows) not calendar days
--
-- A row exists only once every feature is warmed up (~50 trading days).
-- No row means not enough history to score.

with daily as (
    select
        ticker, trade_date, close, volume, sma_20, sma_50, rsi_14, bb_upper, bb_lower, macd_hist,
        close / lag(close) over (partition by ticker order by trade_date) - 1 as ret_1d
    from {{ ref('stg_equities_historical') }}
),

features as (
    select
        ticker,
        trade_date,
        ret_1d,
        close / lag(close, 5) over by_day - 1                       as ret_5d,
        close / lag(close, 20) over by_day - 1                      as ret_20d,
        close / sma_20 - 1                                          as close_vs_sma20,
        close / sma_50 - 1                                          as close_vs_sma50,
        rsi_14,
        macd_hist / close                                           as macd_hist_norm,
        (close - bb_lower) / nullif(bb_upper - bb_lower, 0)         as bb_pct_b,
        case when count(ret_1d) over last_20 = 20
            then stddev_samp(ret_1d) over last_20 end               as realized_vol_20d,
        -- Today against the 20 sessions before it
        case when count(volume) over prior_20 = 20
            then (volume - avg(volume) over prior_20)
                 / nullif(stddev_samp(volume) over prior_20, 0) end as volume_z_20d
    from daily
    window
        by_day   as (partition by ticker order by trade_date),
        last_20  as (partition by ticker order by trade_date rows between 19 preceding and current row),
        prior_20 as (partition by ticker order by trade_date rows between 20 preceding and 1 preceding)
),

market as (
    select trade_date, ret_5d as spy_ret_5d
    from features
    where ticker = 'SPY'
)

select f.*, m.spy_ret_5d
from features f
left join market m using (trade_date)
where f.ret_20d          is not null
  and f.close_vs_sma20   is not null
  and f.close_vs_sma50   is not null
  and f.rsi_14           is not null
  and f.macd_hist_norm   is not null
  and f.bb_pct_b         is not null
  and f.realized_vol_20d is not null
  and f.volume_z_20d     is not null
  and m.spy_ret_5d       is not null
