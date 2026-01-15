-- Gets the delete_log table for the source_schema (as set in dry_config)
{% macro get_delete_log_table() %}
  {%- set delete_log_table = config.get('meta')['source_schema']|string ~ '.delete_logs' -%}
  {{ return(delete_log_table) }}
{% endmacro %}

-- Gets the base_table (as set in dry config) 
{% macro get_base_table() %}
  {%- if config.get('meta')['source_schema'] == 'combined_api_v3' -%}
    {%- set base_table=this.table -%}
  {%- else -%}
    {%- set base_table=config.get('meta')['base_table'] -%}
  {% endif %}
  {{ return(base_table) }}
{% endmacro %}