{% macro full_refresh_pre_hook(table_name=this.table, table_schema=this.schema) %}

{% if flags.FULL_REFRESH %}
  DELETE FROM {{ var('delete_bookmarks_table') }}
  WHERE table_name = '{{ table_name }}'
    AND schema_name = '{{ table_schema }}'
{% endif %}

{% endmacro %}