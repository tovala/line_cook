{% macro search_array(element,array) %}
    BOOLOR_AGG(ARRAY_CONTAINS('{{ element }}'::VARIANT, {{ array }}))
{% endmacro %}
