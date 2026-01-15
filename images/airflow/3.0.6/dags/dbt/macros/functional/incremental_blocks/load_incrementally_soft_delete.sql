{% macro load_incrementally_soft_delete(bookmark='updated') %}
{% if is_incremental() %}
  WHERE {{ bookmark }} >= (SELECT MAX({{ bookmark }}) FROM {{ this }})
  AND deleted IS NULL
{% endif %}
{% endmacro %}