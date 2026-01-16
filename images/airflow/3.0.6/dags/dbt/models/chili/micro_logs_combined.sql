{{ 
  config(
    materialized='incremental',
    full_refresh = false,
    unique_key='micro_log_id',
    schema='kinesis',
    tags=["metadata"]
  )
}}

SELECT
  {{ hash_natural_key('raw_data::STRING')}} AS micro_log_id
  , CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(raw_data:timestamp)) AS time_stamp
  , raw_data 
  , MAX(updated) AS updated
FROM {{ table_reference('micro_logs', 'kinesis') }}
{% if is_incremental() %}
WHERE updated >= (SELECT MAX(updated) FROM {{ this }})
{% endif %}
GROUP BY 1,2,3