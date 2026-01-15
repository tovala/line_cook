-- Inserts records into delete bookmarks table
{% macro log_delete_bookmark(table_name=this.table, table_schema=this.schema) %}
{%- set delete_log_table = get_delete_log_table()-%}
{%- set base_table = get_base_table()-%}
INSERT INTO {{ var('delete_bookmarks_table') }}
SELECT
  '{{ table_name }}' AS table_name
  , '{{ table_schema }}' AS schema_name
  , MAX(delete_time)::TIMESTAMP_NTZ AS delete_bookmark
  , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS bookmark_time
FROM {{ delete_log_table }}
WHERE LOWER(table_name) = '{{ base_table }}'
GROUP BY 1, 2, 4
{% endmacro %}