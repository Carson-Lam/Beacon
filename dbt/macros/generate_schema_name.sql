{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- Use folder schemas as-is instead of dbt default "<target_schema>_<custom>" -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}