select '1min' as bar_size, ticker, bar_start, open, high, low, close
from {{ ref('stg_ohlcv_1min') }}
where not (low <= open and low <= close and open <= high and close <= high)

union all

select '5min', ticker, bar_start, open, high, low, close
from {{ ref('stg_ohlcv_5min') }}
where not (low <= open and low <= close and open <= high and close <= high)

union all

select '1d', ticker, cast(trade_date as timestamptz), open, high, low, close
from {{ ref('stg_equities_historical') }}
-- bar yfinance can return a bar that closes at its high with about 1e-9 price noise
where greatest(open, close) - high > 1e-9 * high or low - least(open, close) > 1e-9 * high