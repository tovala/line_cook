{{
  config(
    alias='micro_logs_blinkup',
    materialized='incremental', 
    unique_key='micro_log_id',
    tags=['oven_logs']
  ) 
}}

SELECT 
  micro_log_id
  , {{ clean_string('raw_data:device_id::STRING') }} AS device_id
  , {{ clean_string('raw_data:model::STRING') }} AS model
  , time_stamp AS blinkup_time
  , updated
FROM {{ source('kinesis', 'micro_logs_combined') }} 
WHERE raw_data:data_type::STRING = 'electricimp'
  AND raw_data:impwebhook::STRING = 'blinkup'
  {% if is_incremental() %}
  AND updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
