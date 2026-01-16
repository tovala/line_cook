
WITH downstream_models AS (
  SELECT 
    model_id 
    , {{ clean_string('value::STRING') }} AS macro_id
  FROM {{ ref('current_models') }}, LATERAL FLATTEN(input => upstream_macros) 
  WHERE ARRAY_SIZE(upstream_macros) > 0
), downstream_tests AS (
  SELECT 
    test_id 
    , {{ clean_string('value::STRING') }} AS macro_id
  FROM {{ ref('current_tests') }}, LATERAL FLATTEN(input => upstream_macros) 
  WHERE ARRAY_SIZE(upstream_macros) > 0
), current_macros AS (
  SELECT 
    mac.macro_id 
    , mac.manifest_id 
    , mac.macro_name 
    , mac.macro_file 
    , mac.upstream_macros
    , mac.updated
  FROM {{ table_reference('macros', 'harvest') }} mac
  WHERE mac.manifest_id = (SELECT MAX_BY(manifest_id, updated) FROM {{ table_reference('macros', 'harvest') }})
), downstream_macros AS (
  SELECT 
    {{ clean_string('value::STRING') }} AS macro_id 
    , ARRAY_AGG(macro_id) AS downstream_macros
  FROM current_macros, LATERAL FLATTEN(INPUT => upstream_macros)
  GROUP BY 1)
SELECT 
  cm.macro_id 
  , cm.manifest_id
  , cm.updated 
  , cm.macro_name 
  , cm.macro_file 
  , cm.upstream_macros
  , ARRAY_SIZE(cm.upstream_macros) AS count_upstream_macros
  , COALESCE(dmac.downstream_macros, ARRAY_CONSTRUCT()) AS downstream_macros
  , ARRAY_SIZE(COALESCE(dmac.downstream_macros, ARRAY_CONSTRUCT())) AS count_downstream_macros
  , ARRAY_AGG(dm.model_id) AS used_by_models
  , ARRAY_SIZE(used_by_models) AS count_used_by_models
  , ARRAY_AGG(dt.test_id) AS used_by_tests
  , ARRAY_SIZE(used_by_tests) AS count_used_by_tests
FROM current_macros cm
LEFT JOIN downstream_macros dmac 
  ON cm.macro_id = dmac.macro_id
LEFT JOIN downstream_models dm 
  ON cm.macro_id = dm.macro_id
LEFT JOIN downstream_tests dt 
  ON cm.macro_id = dt.macro_id
GROUP BY 1,2,3,4,5,6,7,8,9
