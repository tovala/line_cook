{% macro cents_to_usd(cash_amount) %}
    {{ cash_amount }}/100.0
{% endmacro %}