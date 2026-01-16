{{ 
    config(
        materialized='incremental',
        unique_key='manifest_id'
    ) 
}}

SELECT 
  {{ clean_string('data:metadata:invocation_id::STRING') }} AS manifest_id
  , data:child_map AS child_map
  , data:macros AS macros
  , data:nodes AS nodes
  , data:sources AS sources
  , updated
FROM {{ source('dbt_artifacts', 'artifacts') }}
WHERE artifact_type = 'manifest.json'
{% if is_incremental() %}
  AND updated > (SELECT MAX(updated) FROM {{ this }})
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY manifest_id ORDER BY updated DESC) = 1