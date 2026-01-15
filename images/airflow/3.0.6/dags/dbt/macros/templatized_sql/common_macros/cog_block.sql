{% macro cog_block(string) %}
    TRY_TO_DECIMAL(TRIM(REPLACE(raw_data:"{{ string }}"::STRING, '$', '')), 38, 9)
{% endmacro %}