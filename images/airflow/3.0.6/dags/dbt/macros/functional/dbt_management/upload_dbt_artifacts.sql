-- Based on: https://github.com/brooklyn-data/dbt_artifacts/blob/102b63ff532ab79b154a49b000951c7268bb6363/macros/upload_artifacts.sql
{% macro upload_dbt_artifacts(results, filenames=var('dbt_artifacts')['dbt_artifacts_filenames'], prefix=var('dbt_artifacts')['dbt_artifacts_prefix']) %}
    {%- if target.name == 'prod'-%}
        {% set src_dbt_artifacts = source('dbt_artifacts', 'artifacts') %}
        {% set artifact_stage = src_dbt_artifacts.database ~ "." ~ src_dbt_artifacts.schema ~ "." ~ var('dbt_artifacts_stage', 'dbt_artifacts_stage') %}

        {%do log(artifact_stage, info=True) %}

        {% set remove_query %}
            remove @{{ artifact_stage }} pattern='.*.json.gz';
        {% endset %}

        {% do log("Clearing existing files from Stage: " ~ remove_query, info=True) %}
        {% do run_query(remove_query) %}

        {% for filename in filenames %}

            {% set file = filename ~ '.json' %}

            {% set put_query %}
                put file://{{ prefix }}{{ file }} @{{ artifact_stage }} auto_compress=true;
            {% endset %}

            {% do log("Uploading " ~ file ~ " to Stage: " ~ put_query, info=True) %}
            {% do run_query(put_query) %}

            {% set copy_query %}
            begin;
            copy into {{ src_dbt_artifacts }} from
                (
                    select
                    $1 as data,
                    CONVERT_TIMEZONE('UTC', $1:metadata:generated_at::timestamp_tz) as generated_at,
                    metadata$filename as path,
                    regexp_substr(metadata$filename, '([a-z_]+.json)') as artifact_type,
                    case when artifact_type = 'run_results.json'
                        then $1:args:which::string 
                    end as run_or_test,
                    {{ current_timestamp_utc() }} AS updated 
                    from  @{{ artifact_stage }}
                )
                file_format=(type='JSON')
                on_error='skip_file';
            commit;

            {% endset %}

            {% do log("Copying " ~ file ~ " from Stage: " ~ copy_query, info=True) %}
            {% do run_query(copy_query) %}
        
        {% endfor %}

        {% do log("Clearing new files from Stage: " ~ remove_query, info=True) %}
        {% do run_query(remove_query) %}
    {%- endif -%}
{% endmacro %}