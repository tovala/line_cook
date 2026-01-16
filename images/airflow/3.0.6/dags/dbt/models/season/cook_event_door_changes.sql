{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='door_change_id'
    )                     
}}

WITH cook_events AS (
  SELECT 
    cook_cycle_id 
    , unique_cook_cycle_id
    , oven_hardware_id 
    , cook_start_time 
    -- If the cook_end_time is null, add the duration of the cook event to the start time
    -- In the rare event those are also null (54), use 20 minutes
    , COALESCE(cook_end_time, 
               TIMESTAMPADD('seconds', COALESCE(initial_duration, cook_routine_duration, 1200), cook_start_time)) AS cook_end_time
    , COALESCE(LEAD(cook_start_time) OVER (PARTITION BY oven_hardware_id ORDER BY cook_start_time), '9999-12-31') AS next_cook_start_time
  FROM {{ ref('cook_events') }}
  WHERE cook_cycle_id IS NOT NULL
    -- Always true when cook_cycle_id is not null but useful to be explicit
    AND oven_generation IN ('airvala', 'gen_2')
)
SELECT 
  ce.cook_cycle_id 
  , unique_cook_cycle_id
  , dc.oven_event_id AS door_change_id
  , dc.doorstate AS door_state
  , dc.time_stamp AS door_change_time
  , door_change_time < ce.cook_end_time AS is_during_event 
  , door_change_time >= ce.cook_end_time AS is_after_event 
  , CASE WHEN is_during_event 
         THEN TIMESTAMPDIFF('seconds', ce.cook_start_time, door_change_time)
    END AS seconds_since_start
  , CASE WHEN is_after_event 
         THEN TIMESTAMPDIFF('seconds', ce.cook_end_time, door_change_time)
    END AS seconds_after_end 
  , dc.updated AS door_change_updated 
  , NULL AS nth_open_after_complete
FROM cook_events ce 
INNER JOIN {{ table_reference('oven_logs_doorchange') }} dc 
  ON ce.oven_hardware_id = dc.deviceid 
  AND dc.time_stamp >= ce.cook_start_time 
  -- Check that this event isn't associated with another cook event, otherwise give it 10 minutes after the cook event ends. 
  AND dc.time_stamp < LEAST(TIMESTAMPADD('seconds', -60, ce.next_cook_start_time),
                            TIMESTAMPADD('seconds', 600, ce.cook_end_time))
WHERE dc.doorstate IN ('opened', 'closed')
  {% if is_incremental() %}
  AND dc.updated >= (SELECT DATEADD('days', -3, MAX(door_change_updated)) FROM {{ this }})
  AND NOT door_change_id IN (SELECT door_change_id FROM {{ this }})
  {% endif %}
