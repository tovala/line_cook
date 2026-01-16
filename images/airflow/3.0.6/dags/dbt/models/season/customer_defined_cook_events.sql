{{ 
    config(
        materialized='incremental',
        unique_key='cook_event_id',
        tags=["thyme_incremental"]
    ) 
}}

SELECT
  cook_event_id
  , cook_cycle_id
  , unique_cook_cycle_id
  , oven_hardware_id
  , oven_generation
  , hardware_group_name
  , serial_number
  , customer_id
  , session_id
  , cook_start_time
  , cook_end_time
  , cook_duration_seconds
  , cook_end_type
  , barcode
  , normalized_time_remaining_seconds
  , cook_routine
  , cook_routine_duration
  , uses_broil  
  , uses_steam
  , updated
FROM {{ ref('cook_events') }} 
WHERE cook_event_subtype = 'customer_defined'
{% if is_incremental() %}
  AND updated >= (SELECT MAX(updated) FROM {{ this }})
{% endif %}
