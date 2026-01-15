{% macro shipping_term() %}
(SELECT term_id FROM {{ ref('terms') }} WHERE is_being_shipped)
{% endmacro %}
