-- Inserts records into delete bookmarks table
{% macro log_delete_bookmark_deleted_column(table_name=this.table, table_schema=this.schema, source_schema='', source_table='') %}

INSERT INTO {{ var('delete_bookmarks_table') }}
SELECT 
  '{{ table_name }}' AS table_name
  , '{{ table_schema }}' AS schema_name
  , MAX(deleted)::TIMESTAMP_NTZ AS delete_bookmark
  , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS bookmark_time
FROM {{ source_schema }}.{{ source_table }}

{% endmacro %}