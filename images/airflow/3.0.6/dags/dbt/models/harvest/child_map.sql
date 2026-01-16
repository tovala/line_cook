{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

WITH all_nodes AS (
  SELECT 
    manifest_id 
    , updated 
    , key AS model_id
    , ARRAY_DISTINCT(value) AS downstream_nodes
  FROM {{ table_reference('manifest', 'harvest') }}, LATERAL FLATTEN(INPUT => child_map) 
  WHERE key ILIKE 'model.%'
  {% if is_incremental() %}
    AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %})
SELECT 
  {{ hash_natural_key('manifest_id', 'model_id') }} AS id
  , manifest_id 
  , updated
  , model_id
  , ARRAY_AGG(CASE WHEN {{ clean_string('value::STRING') }} LIKE 'model.%'
                   THEN {{ clean_string('value::STRING') }} 
              END) AS downstream_models
FROM all_nodes, LATERAL FLATTEN(INPUT => downstream_nodes)
GROUP BY 1,2,3,4
