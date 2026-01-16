
SELECT 
  model_alias AS table_name
  , model_schema AS table_schema
  , model_description AS table_description
FROM {{ table_reference('current_models', 'harvest') }}
WHERE model_schema IN ('grind', 'sage', 'season')
