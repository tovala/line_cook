
WITH column_descriptions AS (
  SELECT 
    model_id
    , key AS column_name
    , {{ clean_string('value:description::STRING') }} AS column_description 
    , value:tags AS column_tags -- Not currently used, but could be
  FROM {{ ref('current_models') }}, LATERAL FLATTEN(INPUT => model_columns)
  WHERE {{ clean_string('value:description::STRING') }} IS NOT NULL)
SELECT 
  cm.model_id || '.' || LOWER(col.column_name) AS column_id
  , cm.model_id
  , cm.manifest_id
  , cm.updated
  , cm.model_alias
  , cm.model_name
  , cm.model_schema
  , cm.database_table_name
  , LOWER(col.column_name) AS column_name
  , col.ordinal_position AS column_order
  , col.data_type AS column_data_type
  , cd.column_description
  , cd.column_tags
FROM {{ source('information_schema', 'columns') }} col
INNER JOIN {{ ref('current_models') }} cm 
  ON LOWER(col.table_name) = cm.model_alias
  AND LOWER(col.table_schema) = cm.model_schema
LEFT JOIN column_descriptions cd 
  ON cd.model_id = cm.model_id
  AND LOWER(col.column_name) = cd.column_name
