{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

WITH all_nodes AS (
  SELECT 
   {{ hash_natural_key('manifest_id', 'value:unique_id::STRING') }} AS id
    , manifest_id 
    , updated 
    , {{ clean_string('value:unique_id::STRING') }} AS model_id
    , {{ clean_string('value:alias::STRING') }} AS model_alias
    , {{ clean_string('value:name::STRING') }} AS model_name
    , {{ clean_string('value:schema::STRING') }} AS model_schema
    , {{ clean_string('LOWER(value:database::STRING)') }} AS model_database
    , {{ clean_string('value:description::STRING') }} AS model_description
    , {{ clean_string('value:original_file_path::STRING') }} AS model_file
    , SPLIT_PART({{ clean_string('value:patch_path::STRING') }}, '//', 2) AS yml_file
    , value:unrendered_config AS config_block
    , value:config AS full_configs
    , COALESCE(full_configs:full_refresh::BOOLEAN, TRUE) AS full_refresh_enabled
    , {{ clean_string('full_configs:incremental_strategy::STRING') }} AS incremental_strategy
    , {{ clean_string('full_configs:materialized::STRING') }} AS materialized_type
    , {{ clean_string('full_configs:unique_key::STRING') }} AS unique_key
    , value:meta AS meta
    , value:tags AS tags
    , value:depends_on:macros AS upstream_macros
    , value:depends_on:nodes AS upstream_nodes 
    , value:columns AS model_columns
  FROM {{ table_reference('manifest', 'harvest') }}, LATERAL FLATTEN(INPUT => nodes) 
  WHERE {{ clean_string('value:resource_type::STRING') }} = 'model'
  {% if is_incremental() %}
    AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
), references AS (
  SELECT 
    id
    , ARRAY_AGG(CASE WHEN {{ clean_string('value::STRING') }} LIKE 'source%' THEN {{ clean_string('value::STRING') }} END) AS upstream_sources
    , ARRAY_AGG(CASE WHEN {{ clean_string('value::STRING') }} LIKE 'model%' THEN {{ clean_string('value::STRING') }} END) AS upstream_models
  FROM all_nodes, LATERAL FLATTEN(INPUT => upstream_nodes)
  GROUP BY 1
)
SELECT
  an.id 
  , an.manifest_id 
  , an.updated 
  , an.model_id
  , an.model_alias
  , an.model_name
  , an.model_schema
  , an.model_database || '.' || an.model_schema || '.' || an.model_alias AS database_table_name
  , an.model_description
  , an.model_file
  , an.yml_file
  , an.config_block
  , an.full_configs
  , an.full_refresh_enabled
  , CASE WHEN an.materialized_type = 'incremental' 
         THEN COALESCE(an.incremental_strategy, 'default')
    END AS incremental_strategy
  , an.materialized_type
  , an.unique_key
  , an.meta
  , an.tags
  , an.upstream_macros
  , an.model_columns
  , COALESCE(ref.upstream_sources, ARRAY_CONSTRUCT()) AS upstream_sources
  , COALESCE(ref.upstream_models, ARRAY_CONSTRUCT()) AS upstream_models
  , COALESCE(nc.downstream_models, ARRAY_CONSTRUCT()) AS downstream_models
FROM all_nodes an
LEFT JOIN references ref 
  ON an.id = ref.id 
LEFT JOIN {{ table_reference('child_map', 'harvest') }} nc
  ON an.manifest_id = nc.manifest_id
  AND an.model_id = nc.model_id
