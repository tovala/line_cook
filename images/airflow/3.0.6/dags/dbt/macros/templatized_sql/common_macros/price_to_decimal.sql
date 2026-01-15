{% macro price_to_decimal(field_name) %}
    CAST({{ field_name }} AS DECIMAL(10,2))
{% endmacro %}