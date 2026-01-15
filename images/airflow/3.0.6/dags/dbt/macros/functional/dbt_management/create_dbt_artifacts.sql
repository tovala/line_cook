-- Based on: https://github.com/brooklyn-data/dbt_artifacts/blob/102b63ff532ab79b154a49b000951c7268bb6363/macros/create_artifact_resources.sql
{% macro create_dbt_artifacts() %}
    -- IMPORTANT: if you want to update the schema, stage, or table THIS MUST BE UPDATED AND RE-RUN 
    {% set src_dbt_artifacts = source('dbt_artifacts', 'artifacts') %}

    -- 1. Create schema if it doesn't exist 
    {% do log("Creating Schema: " ~ src_dbt_artifacts, info=True) %}
    {{ create_schema(src_dbt_artifacts) }}

    -- 2. Create stage if it doesn't exist 
    {% set artifact_stage = src_dbt_artifacts.database ~ "." ~ src_dbt_artifacts.schema ~ "." ~ var('dbt_artifacts_stage', 'dbt_artifacts_stage') %}
    {% set create_stage_query %}
        create stage if not exists {{ artifact_stage }}
        file_format = (type = json);
    {% endset %}

    {% do log("Creating Stage: " ~ artifact_stage, info=True) %}
    {% do run_query(create_stage_query) %}

    -- 3. Create table to store artifacts if it doesn't exist
    {% set create_table_query %}
        create table if not exists {{ src_dbt_artifacts }} (
            data variant,
            generated_at timestamp_tz,
            path string,
            artifact_type string,
            run_or_test string,
            updated timestamp_tz
        );
    {% endset %}

    {% do log("Creating Artifact Table: " ~ create_table_query, info=True) %}
    {% do run_query(create_table_query) %}

{% endmacro %}
