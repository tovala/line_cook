{% macro latest_completed_term() %}
(SELECT term_id FROM {{ ref('terms') }} WHERE is_latest_completed)
{% endmacro %}