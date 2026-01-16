
SELECT 
  model_alias AS table_name
  , model_schema AS table_schema
  , column_name
  , column_description
  , column_data_type
FROM {{ table_reference('current_columns', 'harvest') }}
WHERE model_schema IN ('grind', 'sage', 'season')
