{% macro enable_reclustering() %}
    {%- set configs = clustering_key_configs() %}
    {%- for config in configs.clustering_keys %}
        {%- set full_table_name = config.schema ~ '.' ~ config.table %}
        {%- set resume_recluster_query = "ALTER TABLE " ~ full_table_name ~ " RESUME RECLUSTER;" %}
        {{ log('Enabling auto-clustering for ' ~ full_table_name, info=True) }}
        {{ run_query(resume_recluster_query) }}
        {{ log('Auto-clustering enabled successfully for ' ~ full_table_name, info=True) }}
    {%- endfor %}
{% endmacro %}
