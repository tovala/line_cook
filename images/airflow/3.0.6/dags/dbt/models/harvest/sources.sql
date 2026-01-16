{{ 
    config(
        materialized='incremental',
        unique_key='id'
    ) 
}}

SELECT 
  {{ hash_natural_key('manifest_id', 'value:unique_id::STRING') }} AS id
  , manifest_id 
  , {{ clean_string('value:unique_id::STRING') }} AS source_id
  , updated 
  , {{ clean_string('value:source_name::STRING') }} AS source_name
  , {{ clean_string('value:name::STRING') }} AS table_name
  , {{ clean_string('value:schema::STRING') }} AS schema_name
  , LOWER({{ clean_string('value:relation_name::STRING') }}) AS database_table_name
  , {{ clean_string('value:loaded_at_field::STRING') }} AS loaded_at_column
  , {{ clean_string('value:freshness::STRING') }} IS NULL AS tested_for_freshness
  , value:freshness:error_after:count::INTEGER AS freshness_error_count
  , {{ clean_string('value:freshness:error_after:period::STRING') }} AS freshness_error_interval
  , value:freshness:warn_after:count::INTEGER AS freshness_warn_count
  , {{ clean_string('value:freshness:warn_after:period::STRING') }} AS freshness_warn_interval
FROM {{ table_reference('manifest', 'harvest') }}, LATERAL FLATTEN(INPUT => sources)
{% if is_incremental() %}
WHERE updated > (SELECT MAX(updated) FROM {{ this }})
{% endif %}
