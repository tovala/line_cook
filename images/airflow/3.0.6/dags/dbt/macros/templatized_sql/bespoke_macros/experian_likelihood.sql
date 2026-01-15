{% macro experian_likelihood(col) %}
    CASE
        WHEN {{ col }} >= 1 AND {{ col }} <= 9 
        THEN 10 - {{ col }}
        ELSE NULL 
    END 
{% endmacro %}
