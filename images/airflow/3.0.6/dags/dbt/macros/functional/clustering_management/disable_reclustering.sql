{% macro disable_reclustering() %}
    {%- set configs = clustering_key_configs() %}
    {%- for config in configs.clustering_keys %}
        {%- set full_table_name = config.schema ~ '.' ~ config.table %}
        {%- set suspend_recluster_query = "ALTER TABLE " ~ full_table_name ~ " SUSPEND RECLUSTER;" %}
        {{ log('Suspending auto-clustering for ' ~ full_table_name, info=True) }}
        {{ run_query(suspend_recluster_query) }}
        {{ log('Auto-clustering suspended successfully for ' ~ full_table_name, info=True) }}
    {%- endfor %}
{% endmacro %}
