{% macro parse_zendesk_ticket_id(zendesk_ticket_id) %}
  NULLIF(TRIM(REGEXP_REPLACE({{ zendesk_ticket_id }}, '[^[:digit:]]', '')), '')::INTEGER
{% endmacro %}
