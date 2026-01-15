{% macro live_term(availability_check=False) %}
(SELECT MIN(term_id) FROM {{ ref('terms') }} WHERE NOT is_past_order_by
  {% if availability_check %}
  AND NOT is_partially_unavailable
  {% endif %}
)
{% endmacro %}