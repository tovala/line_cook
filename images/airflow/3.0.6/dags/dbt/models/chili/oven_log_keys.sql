{{
  config(
    materialized='incremental', 
    unique_key='oven_log_key',
    tags=["metadata"],
    full_refresh = false
  ) 
}}

SELECT 
  key AS oven_log_key
  , MAX(time_stamp) AS maximum_timestamp
FROM {{ source('kinesis', 'oven_logs_combined') }} 
{% if is_incremental() %}
WHERE time_stamp >= (SELECT MAX(maximum_timestamp) FROM {{ this }})
{% endif %}
GROUP BY 1 
  