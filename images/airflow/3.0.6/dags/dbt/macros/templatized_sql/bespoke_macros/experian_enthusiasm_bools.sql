{% macro experian_enthusiasm_bools(col) %}
    COALESCE({{ col }} = 'Y', FALSE)
{% endmacro %}
