{% macro epoch_ms_to_timestamp(createdAt) %}
    TO_TIMESTAMP_TZ({{ createdAt }}/1000)
{% endmacro %}
