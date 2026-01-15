{% macro clean_string(string) %}
    NULLIF(TRIM({{ string }}), '')
{% endmacro %}
