{% macro experian_probability(col) %}
    CASE 
        WHEN {{ col }} = 0 THEN NULL 
        ELSE ROUND((100 - {{ col }}) / 100, 2)
    END 
{% endmacro %}
