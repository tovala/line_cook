{% macro deleted_column_handling_composite_key(table_schema=this.source_schema, table_name=this.base_table, dry_name='', key1='', key2='') %}

DELETE FROM dry.{{ dry_name }}
WHERE ({{ key1 }}::STRING, {{ key2 }}::STRING) IN (
  SELECT {{ key1 }}::STRING, {{ key2 }}::STRING
  FROM {{ table_schema }}.{{ table_name }}
  WHERE deleted IS NOT NULL
  {% if is_incremental() %}
  AND deleted::TIMESTAMP_NTZ > (
    SELECT COALESCE(MAX(delete_bookmark), '0064-07-19')::TIMESTAMP_NTZ AS delete_time
    FROM {{ var('delete_bookmarks_table') }} db 
    WHERE db.table_name = '{{ table_name }}'
      AND db.schema_name = '{{ table_schema }}'
  )
  {% endif %}
)

{% endmacro %}