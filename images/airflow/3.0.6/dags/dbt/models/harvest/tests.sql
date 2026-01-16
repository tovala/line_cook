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
    , {{ clean_string('value:unique_id::STRING') }} AS test_id
    , {{ clean_string('value:name::STRING') }} AS test_name
    , {{ clean_string('value:column_name::STRING') }} AS column_tested
    , {{ clean_string('value:original_file_path::STRING') }} AS test_file
    , value:depends_on:macros AS upstream_macros
    , value:depends_on:nodes AS upstream_nodes
  FROM {{ table_reference('manifest', 'harvest') }}, LATERAL FLATTEN(INPUT => nodes) 
  WHERE {{ clean_string('value:resource_type::STRING') }} = 'test'
  {% if is_incremental() %}
    AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
), references AS (
  SELECT 
    id
    , ARRAY_AGG(CASE WHEN {{ clean_string('value::STRING') }} LIKE 'source%' 
                     THEN {{ clean_string('value::STRING') }} 
                END) AS sources_tested
    , ARRAY_AGG(CASE WHEN {{ clean_string('value::STRING') }} LIKE 'model%' 
                     THEN {{ clean_string('value::STRING') }} 
                END) AS models_tested
  FROM all_nodes, LATERAL FLATTEN(INPUT => upstream_nodes)
  GROUP BY 1
), macros AS (
  SELECT 
    id
    , ARRAY_AGG(CASE WHEN RLIKE({{ clean_string('value::STRING') }}, '^macro.(dbt|spice_rack).test_(.*)') 
                     THEN {{ clean_string('value::STRING') }} 
                END) AS upstream_tests 
    , ARRAY_AGG(CASE WHEN value LIKE 'macro.spice_rack%' AND NOT RLIKE({{ clean_string('value::STRING') }}, '^macro.(dbt|spice_rack).test_(.*)') 
                     THEN value END) AS upstream_macros 
  FROM all_nodes, LATERAL FLATTEN(INPUT => upstream_macros)
  GROUP BY 1
)
SELECT
  an.id 
  , an.manifest_id 
  , an.updated 
  , an.test_id
  , an.test_name
  , an.test_file
  , an.column_tested
  , COALESCE(ref.sources_tested, ARRAY_CONSTRUCT()) AS sources_tested
  , COALESCE(ref.models_tested, ARRAY_CONSTRUCT()) AS models_tested
  , CASE WHEN ARRAY_SIZE(COALESCE(ref.models_tested, ARRAY_CONSTRUCT())) = 1
         THEN {{ clean_string('ref.models_tested[0]::STRING') }}
    END AS model_tested
  , COALESCE(mac.upstream_macros, ARRAY_CONSTRUCT()) AS upstream_macros
  , CASE WHEN RLIKE(an.test_file, 'tests/(.*).sql')
         THEN 'custom'
         WHEN ARRAY_SIZE(mac.upstream_tests) = 1 
              AND {{ clean_string('mac.upstream_tests[0]::STRING') }} LIKE 'macro.dbt.test_%' 
         THEN 'generic'
         WHEN ARRAY_SIZE(mac.upstream_tests) = 1 
              AND {{ clean_string('mac.upstream_tests[0]::STRING') }} LIKE 'macro.spice_rack.test_%'
         THEN 'custom_generic'
    END AS test_type
  , CASE WHEN test_type IN ('generic', 'custom_generic')
         THEN SPLIT_PART(REGEXP_SUBSTR({{ clean_string('mac.upstream_tests[0]::STRING') }}, 'test_(.*)'), 'test_', 2)
    END AS generic_validation_type
  , CASE WHEN an.column_tested IS NOT NULL 
         THEN 'column'
         -- Only one model is being tested 
         WHEN ARRAY_SIZE(COALESCE(ref.models_tested, ARRAY_CONSTRUCT())) = 1 
              AND ARRAY_SIZE(COALESCE(ref.sources_tested, ARRAY_CONSTRUCT())) = 0
         THEN 'table'
         -- Only one source is being tested
         WHEN ARRAY_SIZE(COALESCE(ref.sources_tested, ARRAY_CONSTRUCT())) = 1
              AND ARRAY_SIZE(COALESCE(ref.models_tested, ARRAY_CONSTRUCT())) = 0
         THEN 'source'
         -- Some combination of models and sources are being tested
         WHEN ARRAY_SIZE(COALESCE(ref.sources_tested, ARRAY_CONSTRUCT())) 
              + ARRAY_SIZE(COALESCE(ref.models_tested, ARRAY_CONSTRUCT())) > 1
         THEN 'multi'
         WHEN test_name LIKE 'dry_schema%'
         THEN 'schema'
    END AS validation_level
FROM all_nodes an
LEFT JOIN references ref 
  ON an.id = ref.id 
LEFT JOIN macros mac 
  ON an.id = mac.id 
