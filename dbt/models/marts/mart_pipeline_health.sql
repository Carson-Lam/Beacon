-- Row count and latest timestamp per silver source for Pipeline Health screen
-- If the table exists it still gets a row

{%- set tables = [
    ('ohlcv_crypto_1min',    'window_start'),
    ('ohlcv_crypto_5min',    'window_start'),
    ('ohlcv_equities_1min',  'window_start'),
    ('ohlcv_equities_5min',  'window_start'),
    ('indicators_crypto',    'window_start'),
    ('indicators_equities',  'window_start'),
    ('equities_historical',  '"date"'),
    ('sentiment_scored',     '"timestamp"'),
    ('sentiment_aggregates', 'computed_at'),
] %}

with per_table as (
{%- for table_name, ts_col in tables %}
    {%- if delta_source_exists('silver', table_name) %}
    select
        '{{ table_name }}'                      as source_table,
        true                                    as table_exists,
        count(*)                                as row_count,
        cast(max({{ ts_col }}) as timestamptz)  as latest_at
    from {{ source('silver', table_name) }}
    {%- else %}
    select
        '{{ table_name }}'                      as source_table,
        false                                   as table_exists,
        0                                       as row_count,
        cast(null as timestamptz)               as latest_at
    {%- endif %}
    {% if not loop.last %}union all{% endif %}
{%- endfor %}
)

select
    source_table,
    table_exists,
    row_count,
    latest_at,
    date_diff('minute', latest_at, now())   as minutes_since_latest,
    now()                                   as checked_at
from per_table