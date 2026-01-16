
WITH downstream_models AS (
  SELECT 
    model_id 
    , {{ clean_string('value::STRING') }} AS source_id 
  FROM {{ ref('current_models') }}, LATERAL FLATTEN(input => upstream_sources) 
  WHERE ARRAY_SIZE(upstream_sources) > 0
), downstream_tests AS (
  SELECT 
    test_id 
    , {{ clean_string('value::STRING') }} AS source_id
  FROM {{ ref('current_tests') }}, LATERAL FLATTEN(input => sources_tested) 
  WHERE ARRAY_SIZE(sources_tested) > 0
)
SELECT 
  so.source_id 
  , so.manifest_id
  , so.updated
  , so.source_name 
  , so.table_name 
  , so.schema_name 
  , so.database_table_name 
  , so.loaded_at_column
  , so.tested_for_freshness
  , so.freshness_error_count 
  , so.freshness_error_interval
  , so.freshness_warn_count 
  , so.freshness_warn_interval 
  , cm.model_id IS NOT NULL AS is_model
  , cm.model_id
  , ARRAY_AGG(dm.model_id) AS downstream_models
  , ARRAY_SIZE(ARRAY_AGG(dm.model_id)) AS count_downstream_models
  , ARRAY_AGG(dt.test_id) AS source_tests
  , ARRAY_SIZE(ARRAY_AGG(dt.test_id)) AS count_source_tests
FROM {{ table_reference('sources', 'harvest') }} so
LEFT JOIN downstream_models dm 
  ON so.source_id = dm.source_id
LEFT JOIN downstream_tests dt 
  ON so.source_id = dt.source_id
LEFT JOIN {{ ref('current_models') }} cm 
  ON so.database_table_name = cm.database_table_name
WHERE so.manifest_id = (SELECT MAX_BY(manifest_id, updated) FROM {{ table_reference('sources', 'harvest') }})
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
