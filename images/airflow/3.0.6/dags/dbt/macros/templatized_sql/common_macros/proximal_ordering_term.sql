{% macro proximal_ordering_term() %}
(SELECT MAX(term_id) FROM {{ ref('terms') }} WHERE is_past_order_by)
{% endmacro %}