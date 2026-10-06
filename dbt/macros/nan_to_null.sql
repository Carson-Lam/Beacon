{% macro nan_to_null(col) -%}
    {#- Streaming indicators store warm-up rows as NaN (pandas -> Spark). DuckDB sorts NaN
        above every number, so range tests would flag them. missing, not invalid. -#}
    case when isnan({{ col }}) then null else {{ col }} end
{%- endmacro %}