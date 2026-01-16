{{
  config(
    materialized='incremental', 
    unique_key='load_id',
    tags=["metadata"],
    meta = {'source_schema':'combined_api_v3'}
  ) 
}}

SELECT 
  {{ hash_natural_key('file_name', 'last_load_time::STRING') }} AS load_id
  , schema_name
  , file_name
  , table_name 
  , last_load_time 
  , status 
  , row_count
  , row_parsed
  , error_count
FROM {{ source('information_schema', 'load_history') }}

{{ load_incrementally(bookmark='last_load_time') }}