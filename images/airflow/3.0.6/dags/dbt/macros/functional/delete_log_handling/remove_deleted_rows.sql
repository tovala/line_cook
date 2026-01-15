-- Deletes rows that exist in the dry table that also exist in the delete logs table
{% macro remove_deleted_rows(table_schema=this.schema, table_name=this.name, primary_key=config.get('unique_key')) %}
{%- set delete_log_table = get_delete_log_table()-%}
{%- set base_table = get_base_table()-%}
DELETE FROM {{ table_schema }}.{{ table_name }}
WHERE {{ primary_key }}::STRING IN (
  SELECT 
    DISTINCT dl.row_id AS id 
  FROM {{ delete_log_table }} dl 
  WHERE dl.table_name = '{{ base_table }}'
  {% if is_incremental() %}
    AND delete_time::TIMESTAMP_NTZ > (
      SELECT
          COALESCE(MAX(delete_bookmark), '0064-07-19')::TIMESTAMP_NTZ AS delete_time
      FROM {{ var('delete_bookmarks_table') }} db 
      WHERE db.table_name = '{{ table_name }}'
        AND db.schema_name = '{{ table_schema }}'
    )
  {% endif %}
)
{% endmacro %}