
WITH test_metrics AS (
  SELECT 
    test_id 
    , test_result_id
    , run_results_id
    , updated
    , test_result
    , total_time_seconds
    , pipeline_name 
    , selector_name 
    , dbt_target
    , SUM(CASE WHEN test_result = 'pass' THEN 1 ELSE 0 END) OVER 
          (PARTITION BY test_id ORDER BY updated ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_passes
    , SUM(CASE WHEN test_result <> 'pass' THEN 1 ELSE 0 END) OVER 
          (PARTITION BY test_id ORDER BY updated ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_failures
    , SUM(CASE WHEN test_result = 'pass' THEN 1 ELSE 0 END) OVER 
          (PARTITION BY test_id ORDER BY updated ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS recent_passes
    , SUM(CASE WHEN test_result <> 'pass' THEN 1 ELSE 0 END) OVER 
          (PARTITION BY test_id ORDER BY updated ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS recent_failures
    , AVG(total_time_seconds) OVER
          (PARTITION BY test_id ORDER BY updated ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS average_runtime_seconds 
  FROM {{ table_reference('etl_tests', 'harvest') }}
  GROUP BY 1,2,3,4,5,6,7,8,9
  QUALIFY ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY updated DESC) = 1 
)
SELECT 
  te.test_id
  , te.manifest_id
  , te.updated
  , te.test_name
  , te.test_file
  , te.column_tested
  , te.sources_tested
  , te.models_tested
  , te.model_tested
  , te.upstream_macros
  , te.test_type
  , te.generic_validation_type
  , te.validation_level
  , tm.test_result_id
  , tm.run_results_id
  , tm.pipeline_name 
  , tm.selector_name 
  , tm.dbt_target
  , tm.test_result AS most_recent_result
  , tm.total_time_seconds AS most_recent_time_seconds
  , tm.total_passes 
  , tm.total_failures 
  , tm.total_passes/(tm.total_passes + tm.total_failures)*100 AS total_success_percentage 
  , tm.recent_passes 
  , tm.recent_failures 
  , tm.recent_passes/(tm.recent_passes + tm.recent_failures)*100 AS recent_success_percentage
  , tm.average_runtime_seconds
FROM {{ table_reference('tests', 'harvest') }} te
LEFT JOIN test_metrics tm 
  ON te.test_id = tm.test_id
WHERE manifest_id = (SELECT MAX_BY(manifest_id, updated) FROM {{ table_reference('tests', 'harvest') }})
