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
  , oe.event_time AS allocation_time
  , oe.local_event_time AS local_allocation_time
  , oe.serial_number
  , oe.customer_id
  , oe.oven_registration_id
  , ha.allocated AS allocated_memory
  , ha.freememory AS free_memory
  , ha.currentstate AS oven_state
  , oe.updated
FROM {{ table_reference('oven_events', 'grind') }} oe 
LEFT JOIN {{ table_reference('oven_logs_huge_allocation') }} ha 
  ON oe.oven_event_id = ha.oven_event_id
WHERE oe.key = 'huge_allocation'
  {% if is_incremental() %}
  AND oe.updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
