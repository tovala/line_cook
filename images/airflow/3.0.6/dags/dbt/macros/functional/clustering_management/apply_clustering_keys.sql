{% macro apply_clustering_keys() %}
  
  {%- set clustering_configs = clustering_key_configs() -%}
  {% for table_config in clustering_configs.clustering_keys %}
    {% set schema_name = table_config.schema %}
    {% set table_name = table_config.table %}
    {% set keys = table_config.keys %}
    {% set full_table_name = schema_name ~ '.' ~ table_name %}
    {{ log('The table name this round ' ~ table_name, info=True) }}
    {% set formatted_keys_string = keys | join(', ') %}
    {{ log('The key names this round: (' ~ formatted_keys_string ~ ')', info=True) }}

    -- Apply clustering keys if no effective clustering is set
    {{ log('Applying clustering keys to ' ~ table_name, info=True) }} 
    {% set alter_table_query %}  
        ALTER TABLE {{ full_table_name }}
        CLUSTER BY ({{formatted_keys_string}});
    {% endset %}
    {{ log('query face ' ~ alter_table_query, info=True) }} 
    {% do run_query(alter_table_query) %}    
    {{ log('New table ' ~ table_name ~ ' has been clustered now!.', info=True) }}
  {% endfor %}
{% endmacro %}