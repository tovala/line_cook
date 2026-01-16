
WITH model_metrics AS (
  SELECT 
    model_id
    , model_build_id
    , run_results_id
    , updated
    , model_status
    , total_time_seconds
    , pipeline_name 
    , selector_name 
    , dbt_target 
    , AVG(total_time_seconds) OVER
          (PARTITION BY model_id ORDER BY updated ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS average_runtime_seconds 
    , MAX(updated) OVER (PARTITION BY model_id ORDER BY updated) AS last_created_time 
    , MIN(updated) OVER (PARTITION BY model_id ORDER BY updated) AS first_created_time
  FROM {{ table_reference('etl_model_builds', 'harvest') }}
  GROUP BY 1,2,3,4,5,6,7,8,9
  QUALIFY ROW_NUMBER() OVER (PARTITION BY model_id ORDER BY updated DESC) = 1
), current_models AS (
  SELECT 
    mo.manifest_id
    , mo.updated
    , mo.model_id
    , mo.model_alias
    , mo.model_name
    , mo.model_schema
    , mo.database_table_name
    , mo.model_description
    , mo.model_file
    , mo.yml_file
    , mo.config_block
    , mo.full_configs
    , mo.full_refresh_enabled
    , mo.incremental_strategy
    , mo.materialized_type
    , mo.unique_key
    , mo.meta
    , mo.tags
    , mo.upstream_macros
    , mo.upstream_sources
    , mo.upstream_models 
    , mo.downstream_models 
    , mo.model_columns
  FROM {{ table_reference('models', 'harvest') }} mo
  WHERE mo.manifest_id = (SELECT MAX_BY(manifest_id, updated) FROM {{ table_reference('models', 'harvest') }})
), all_schema_deps AS (
  SELECT 
    model_id 
    , {{ clean_string('value::STRING') }} AS dependency_id
  FROM current_models, LATERAL FLATTEN(input => upstream_models)  
), model_to_schema_deps AS (
  SELECT 
    cm1.model_id 
    , ARRAY_UNIQUE_AGG(cm2.model_schema) AS upstream_schemas
  FROM current_models cm1 
  LEFT JOIN all_schema_deps sd 
    ON cm1.model_id = sd.model_id 
  LEFT JOIN current_models cm2 
    ON sd.model_id = cm2.model_id
  GROUP BY 1
), model_tests AS (
  SELECT 
    model_id 
    , ARRAY_AGG(test_id) AS model_tests
  FROM {{ ref('current_models_to_tests') }}
  GROUP BY 1
) 
SELECT 
  cm.model_id
  , cm.manifest_id
  , cm.updated
  , cm.model_alias
  , cm.model_name
  , cm.model_schema
  , cm.database_table_name
  , cm.model_description
  , cm.model_file
  , cm.yml_file
  , cm.config_block
  , cm.full_configs
  , cm.full_refresh_enabled
  , cm.incremental_strategy
  , cm.materialized_type
  , cm.unique_key
  , cm.meta
  , cm.tags
  , cm.downstream_models
  , ARRAY_SIZE(cm.downstream_models) AS count_downstream_models
  , cm.upstream_models
  , ARRAY_SIZE(cm.upstream_models) AS count_upstream_models
  , cm.upstream_macros
  , ARRAY_SIZE(cm.upstream_macros) AS count_upstream_macros
  , cm.upstream_sources
  , ARRAY_SIZE(cm.upstream_sources) AS count_upstream_sources
  , count_upstream_sources + count_upstream_models AS total_upstream_tables
  , COALESCE(msd.upstream_schemas, ARRAY_CONSTRUCT()) AS upstream_schemas  
  , ARRAY_SIZE(COALESCE(msd.upstream_schemas, ARRAY_CONSTRUCT())) AS count_upstream_schemas
  , COALESCE(mt.model_tests, ARRAY_CONSTRUCT()) AS model_tests
  , ARRAY_SIZE(COALESCE(mt.model_tests, ARRAY_CONSTRUCT())) AS count_model_tests
  , mm.model_build_id
  , mm.run_results_id
  , mm.model_status AS most_recent_status
  , mm.total_time_seconds AS most_recent_time_seconds
  , mm.pipeline_name 
  , mm.selector_name 
  , mm.dbt_target 
  , mm.average_runtime_seconds 
  , mm.last_created_time 
  , mm.first_created_time
  , cm.model_columns
FROM current_models cm 
LEFT JOIN model_metrics mm 
  ON cm.model_id = mm.model_id
LEFT JOIN model_to_schema_deps msd 
  ON cm.model_id = msd.model_id
LEFT JOIN model_tests mt 
  ON cm.model_id = mt.model_id 
