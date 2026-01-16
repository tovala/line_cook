{{ 
  config(
    materialized='incremental',
    full_refresh = false,
    schema='kinesis',
    tags=["metadata"],
    unique_key='oven_event_id'
  )
}}

SELECT
  {{ hash_natural_key('raw_data::STRING')}} AS oven_event_id
  , {{ clean_string('raw_data:agentid::STRING') }} AS agentid 
  , {{ clean_string('raw_data:deviceGroupID::STRING') }} AS deviceGroupID
  , {{ clean_string('raw_data:deviceGroupName::STRING') }} AS deviceGroupName
  , COALESCE({{ clean_string('raw_data:deviceid::STRING') }}, {{ clean_string('raw_data:deviceID::STRING') }}) AS deviceid
  , COALESCE({{ clean_string('raw_data:key::STRING') }}, {{ clean_string('raw_data:eventKey::STRING') }}) AS key
  , {{ clean_string('raw_data:serialNumber::STRING') }} AS serialNumber
  , COALESCE({{ clean_string('raw_data:sessionID::STRING') }}, {{ clean_string('raw_data:sessionid::STRING') }}) AS sessionID
  , {{ clean_string('raw_data:source::STRING') }} AS source
  , COALESCE(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(raw_data:timestamp)), CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(raw_data:eventTimeMs::NUMBER, 3))) AS time_stamp
  , raw_data 
  , MAX(updated) AS updated
FROM {{ table_reference('oven_logs', 'kinesis') }}
{% if is_incremental() %}
WHERE updated >= (SELECT MAX(updated) FROM {{ this }})
{% endif %}
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
