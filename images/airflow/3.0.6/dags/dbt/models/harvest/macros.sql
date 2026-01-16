{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

WITH all_macros AS (
  SELECT  
    {{ hash_natural_key('manifest_id', 'value:unique_id::STRING') }} AS id
    , manifest_id 
    , updated 
    , {{ clean_string('value:unique_id::STRING') }} AS macro_id
    , {{ clean_string('value:name::STRING') }} AS macro_name
    , {{ clean_string('value:original_file_path::STRING') }} AS macro_file
    , value:depends_on:macros AS all_upstream_macros
  FROM {{ table_reference('manifest', 'harvest') }}, LATERAL FLATTEN(INPUT => macros) 
  WHERE key LIKE 'macro.spice_rack%'
  {% if is_incremental() %}
    AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
), filtered_upstream_macros AS (
  SELECT 
    id 
    , ARRAY_AGG(CASE WHEN value LIKE 'macro.spice_rack%' THEN value END) AS upstream_macros
  FROM all_macros, LATERAL FLATTEN(INPUT => all_upstream_macros)
  GROUP BY 1)
SELECT 
  am.id 
  , am.manifest_id
  , am.updated 
  , am.macro_id
  , am.macro_name
  , am.macro_file
  , COALESCE(fum.upstream_macros, ARRAY_CONSTRUCT()) AS upstream_macros
FROM all_macros am 
LEFT JOIN filtered_upstream_macros fum 
  ON am.id = fum.id
