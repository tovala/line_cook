{% macro dry_config(table_name, tags=[], primary_key='id', load_mode='incremental'
  , post_hooks = ["{{ remove_deleted_rows() }}","{{ log_delete_bookmark() }}"]
  , pre_hooks = ["{{ full_refresh_pre_hook() }}"] 
  , meta = {'source_schema':'combined_api_v3'})%}
{{
  config(
    alias=table_name,
    materialized=load_mode, 
    unique_key=primary_key,
    pre_hook=pre_hooks,
    post_hook=post_hooks, 
    meta=meta,
    tags=tags
  ) 
}}
{% endmacro %}
