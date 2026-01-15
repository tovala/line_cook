{% macro hash_prospect_id() %}
({{ hash_natural_key('COALESCE(lead_id::STRING, email)') }})
{% endmacro %}
