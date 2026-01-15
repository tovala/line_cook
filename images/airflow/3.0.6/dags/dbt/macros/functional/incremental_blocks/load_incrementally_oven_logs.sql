{% macro load_incrementally_oven_logs(bookmark='updated') %}

{% if is_incremental() %}
  AND {{ bookmark }} > (SELECT MAX({{ bookmark }}) FROM {{ this }})
{% endif %}

{% endmacro %}