{{ 
    config(
        materialized='incremental',
        unique_key='test_result_id'
    ) 
}}

SELECT 
  {{ hash_natural_key('run_results_id', 'value:unique_id::STRING') }} AS test_result_id
  , {{ clean_string('value:unique_id::STRING') }} AS test_id
  , {{ clean_string('value:status::STRING') }} AS test_result
  , value:failures::INTEGER AS count_failing_rows
  , value:execution_time::FLOAT AS total_time_seconds 
  , {{ clean_string('value:adapter_response:query_id::STRING') }} AS snowflake_query_id
  , run_results_id
  , pipeline_name
  , selector_name
  , dbt_target
  , updated
FROM {{ table_reference('etl_run_results', 'harvest') }}, LATERAL FLATTEN(results) 
WHERE dbt_command_type = 'test' 
  {% if is_incremental() %}
  AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
