{% macro dry_config_oven_logs(table_name, load_mode='incremental', tag_list = ["oven_logs"]) %}
{{
  config(
    alias=table_name,
    materialized=load_mode, 
    tags=tag_list,
    unique_key='oven_event_id'
  ) 
}}
{% endmacro %}