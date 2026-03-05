{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# -- If I don't specify a schema, use the default from profiles.yml #}
        {{ default_schema }}
    {%- else -%}
        {# -- If I DO specify a schema (like 'gold'), use exactly that name! #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}