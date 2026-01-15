{%- macro search_string(base_field, search_terms) -%} 
    {%- if search_terms is string -%}
        {%- set search_string = search_terms -%}
    {%- else -%}
        {%- set search_string = search_terms|join('|') -%}
    {%- endif -%}
    REGEXP_LIKE({{base_field}}, '.*\\b({{ search_string }})\\b.*', 'i')
{%- endmacro -%}