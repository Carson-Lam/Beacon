{% macro delta_source_exists(source_name, table_name) %}
    {#- True if the source's Delta table exists on disk. At parse time, assume it does,
        so every source still appears in the lineage graph. -#}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {%- set node = graph.sources['source.' ~ project_name ~ '.' ~ source_name ~ '.' ~ table_name] -%}
    {%- set result = run_query("select count(*) from glob('" ~ node.meta.delta_path ~ "/_delta_log/*.json')") -%}
    {{ return(result.columns[0].values()[0] > 0) }}
{% endmacro %}


{% macro union_existing_sources(source_name, tables) %}
    {#- tables: dict of source table name -> asset_class label. Sources whose Delta table
        doesn't exist yet are skipped -#}
    {%- set present = [] -%}
    {%- for table_name, asset_class in tables.items() -%}
        {%- if delta_source_exists(source_name, table_name) -%}
            {%- do present.append((table_name, asset_class)) -%}
        {%- else -%}
            {%- do log("Skipping " ~ source_name ~ "." ~ table_name ~ ": Delta table not created yet", info=true) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if present | length == 0 -%}
        {%- do exceptions.raise_compiler_error("None of these Delta tables exist yet: " ~ (tables.keys() | list | join(", "))) -%}
    {%- endif -%}
    {%- for table_name, asset_class in present %}
    select '{{ asset_class }}' as asset_class, * from {{ source(source_name, table_name) }}
    {% if not loop.last %}union all by name{% endif %}
    {%- endfor %}
{% endmacro %}