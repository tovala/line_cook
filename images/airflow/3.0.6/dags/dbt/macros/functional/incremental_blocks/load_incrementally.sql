{% macro load_incrementally(bookmark='updated', primary_key=config.get('unique_key')) %}
{%- set delete_log_table = get_delete_log_table()-%}
{%- set base_table = get_base_table()-%}
{% if is_incremental() %}
  -- COALESCE is included in case there are no records in the table but it does exist
  WHERE {{ bookmark }} >= COALESCE((SELECT MAX({{ bookmark }}) FROM {{ this }}), '1991-05-17')
    {%- if primary_key != None -%}
    AND NOT {{ primary_key }}::STRING IN (SELECT row_id FROM {{ delete_log_table }} WHERE TABLE_NAME = '{{ base_table }}')
    {% endif %}
{% endif %}
{% endmacro %}