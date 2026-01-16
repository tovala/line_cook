{{ 
    config(
        materialized='incremental',
        unique_key='run_results_id'
    ) 
}}

SELECT 
  {{ clean_string('data:metadata:invocation_id::STRING') }} AS run_results_id
  , {{ clean_string('data:metadata:dbt_version::STRING') }} AS dbt_version
  , TRY_TO_TIMESTAMP_TZ(data:metadata:generated_at::STRING) AS generated_time
  , data:elapsed_time::FLOAT AS total_duration_seconds
  , data:args AS dbt_arguments
  -- NOTE: 'selector_name' is only valid if a selector is used for the run or test. If a selector is not used, the selection filters are included in dbt_arguments:select
  , COALESCE({{ clean_string('dbt_arguments:selector_name::STRING') }},  
             {{ clean_string('dbt_arguments:selector::STRING') }}) AS selector_name
  , LOWER(REGEXP_SUBSTR({{ clean_string('dbt_arguments:profiles_dir::STRING') }}, '(Chili|Thyme)')) AS pipeline_name
  , {{ clean_string('dbt_arguments:target::STRING') }} AS dbt_target
  , run_or_test AS dbt_command_type
  , updated
  , data:results AS results
  , ARRAY_SIZE(results) AS num_queries_run -- Number of tables built or tests run
FROM {{ source('dbt_artifacts', 'artifacts') }}
WHERE artifact_type = 'run_results.json'
{% if is_incremental() %}
  AND updated > (SELECT MAX(updated) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY run_results_id ORDER BY updated DESC) = 1
