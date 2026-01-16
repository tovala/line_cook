{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='oven_event_id'
    ) 
}}

SELECT 
  oe.oven_event_id
  , oe.agent_id
  , oe.hardware_group_id
  , oe.hardware_group_name
  , oe.oven_hardware_id
  , oe.session_id
  , oe.event_time AS crash_time
  , oe.local_event_time AS local_crash_time
  , oe.serial_number
  , oe.customer_id
  , oe.oven_registration_id
  , ge.error AS error_message
  , oe.updated
FROM {{ table_reference('oven_events', 'grind') }} oe 
LEFT JOIN {{ table_reference('oven_logs_global_exception') }} ge 
  ON oe.oven_event_id = ge.oven_event_id
WHERE oe.key = 'global_exception'
  {% if is_incremental() %}
  AND oe.updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}