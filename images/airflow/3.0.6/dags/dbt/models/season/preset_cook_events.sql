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
  , CASE WHEN barcode = 'PRESET-FEC787A4-041F-4337-B3FE-F24B719B2A19'
         THEN 'cleaning'
         WHEN barcode LIKE 'MANUAL-HEAT%' OR barcode LIKE 'MANUAL-MINI-MAGIC%'
         THEN 'heat'
         WHEN barcode LIKE 'MANUAL-TOAST%' OR barcode LIKE 'MANUAL-MINI-TOAST%'
         THEN 'toast'
    END AS preset_category 
FROM {{ ref('cook_events') }} 
WHERE cook_event_subtype = 'preset'
{% if is_incremental() %}
  AND updated >= (SELECT MAX(updated) FROM {{ this }})
{% endif %}
