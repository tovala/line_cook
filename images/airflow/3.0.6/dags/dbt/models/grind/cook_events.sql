
WITH gen2_and_airvala_ovens AS  (
  SELECT
    cs.cook_event_id
    , cs.cook_cycle_id
    , cs.unique_cook_cycle_id
    , cs.oven_hardware_id
    , cs.oven_generation
    , cs.hardware_group_name
    , cs.serial_number
    , cs.is_test_oven
    , cs.is_internal_account
    , cs.customer_id
    , uas.user_status AS subscription_status
    , cs.session_id
    , cs.cook_start_time
    , CASE WHEN cs.hardware_start_time IS NOT NULL AND COALESCE(ccom.hwtimestamp, ccan.hwtimestamp) IS NOT NULL 
           THEN COALESCE(ccom.hwtimestamp, ccan.hwtimestamp) - cs.hardware_start_time
      END AS hardware_duration_ms
    , cs.local_cook_start_time
    , cs.time_of_meal_category
    , cs.cook_event_subtype
    , cs.barcode
    , cs.preset_or_steadystate
    , cs.steadystate_type
    , cs.initial_duration 
    , cs.initial_temperature
    , cs.cook_routine
    , cs.cook_routine_duration
    , cs.uses_broil 
    , cs.uses_steam
    , cs.is_test_event
    , cs.event_timezone
    , cs.expected_end_time
    , COALESCE(ccom.time_stamp, ccan.time_stamp) AS cook_end_time 
    -- We added 10 seconds to the expected end time to account for lag between the event ending and the data being sent
    -- The 'hardware time' for an event differs from the time stamp by less than 10 seconds 99.9% of the time which is how 10 seconds was chosen
    , cook_end_time > COALESCE(TIMESTAMPADD('seconds', 10, cs.expected_end_time), '9999-12-31') AS has_exceeded_expected_time
    , CASE WHEN has_exceeded_expected_time
           THEN TIMESTAMPDIFF('seconds', expected_end_time, cook_end_time) 
      END AS seconds_past_expected_end_time
    , CONVERT_TIMEZONE(cs.event_timezone, cook_end_time) AS local_cook_end_time
    , CASE WHEN ccom.oven_event_id IS NOT NULL AND ccan.oven_event_id IS NOT NULL 
           -- Sanity check: this should never be both
           THEN 'both'
           WHEN ccom.oven_event_id IS NOT NULL 
           THEN 'cook_completed'
           WHEN ccan.oven_event_id IS NOT NULL 
           THEN 'cook_canceled'
           -- TODO: Hone in on possible reasons for terminations - missed interupts, wifi disconnect etc. 
           -- We could probably calculate when the cook event SHOULD have ended and go from there 
           ELSE 'unknown_termination'
      END AS cook_end_type
    , TIMESTAMPDIFF('seconds', cs.cook_start_time, cook_end_time) AS cook_duration_seconds
    , ccan.timeremaining AS time_remaining
    -- Sometimes the time_remaining gets returned as an insanely high or negative value
    -- when that is the case, it is helpful to have a normalized value (how many seconds were actually left when the cook event was canceled)
    , CASE WHEN (cs.cook_routine_duration - cook_duration_seconds - time_remaining) <> 0
             AND time_remaining IS NOT NULL AND cs.cook_routine_duration IS NOT NULL 
           THEN cs.cook_routine_duration - cook_duration_seconds
           ELSE time_remaining 
      END AS normalized_time_remaining_seconds
    , GREATEST(COALESCE(cs.updated, '1991-05-17'), COALESCE(ccom.updated, '1991-05-17'), COALESCE(ccan.updated, '1991-05-17')) AS updated
  FROM {{ table_reference('cook_event_starts', 'wash') }} cs
  LEFT JOIN {{ table_reference('oven_logs_cookingcomplete') }} ccom
    ON cs.unique_cook_cycle_id = ccom.unique_cook_cycle_id
  LEFT JOIN {{ table_reference('oven_logs_cookingcanceled') }} ccan 
    ON cs.unique_cook_cycle_id = ccan.unique_cook_cycle_id
  LEFT JOIN {{ ref('user_activity_statuses') }} uas
    ON cs.customer_id = uas.customer_id
    AND cs.cook_start_time >= uas.status_start_time
    AND cs.cook_start_time < COALESCE(uas.status_end_time,'9999-12-31')
  WHERE cs.oven_generation IN ('gen_2', 'airvala')
), gen1_ovens AS (
  SELECT
    cs.cook_event_id
    , cs.cook_cycle_id
    , cs.unique_cook_cycle_id
    , cs.oven_hardware_id
    , cs.oven_generation
    , cs.hardware_group_name
    , cs.serial_number
    , cs.is_test_oven
    , cs.is_internal_account
    , cs.customer_id
    , uas.user_status AS subscription_status
    , cs.session_id
    , cs.cook_start_time
    , NULL AS hardware_duration_ms
    , cs.local_cook_start_time
    , cs.time_of_meal_category
    , cs.cook_event_subtype
    , cs.barcode
    , cs.preset_or_steadystate
    , cs.steadystate_type
    , cs.initial_duration 
    , cs.initial_temperature
    , cs.cook_routine
    , cs.cook_routine_duration
    , cs.uses_broil 
    , cs.uses_steam
    , cs.is_test_event
    , cs.event_timezone
    , NULL AS expected_end_time
    , MIN(cend.time_stamp) AS cook_end_time 
    , NULL AS has_exceeded_expected_time
    , NULL AS seconds_past_expected_end_time
    , CONVERT_TIMEZONE(cs.event_timezone, cook_end_time) AS local_cook_end_time
    , CASE WHEN cook_end_time IS NOT NULL 
           THEN 'cook_completed'
           ELSE 'unknown_termination'
      END AS cook_end_type
    , TIMESTAMPDIFF('seconds', cs.cook_start_time, cook_end_time) AS cook_duration_seconds
    , NULL AS time_remaining
    , NULL AS normalized_time_remaining_seconds
    , GREATEST(COALESCE(MIN(cs.updated), '1991-05-17'), COALESCE(MIN(cend.updated), '1991-05-17')) AS updated
  FROM {{ table_reference('cook_event_starts', 'wash') }} cs
  LEFT JOIN {{ table_reference('oven_logs_cookingended') }} cend
  ON cs.session_id = cend.sessionid 
    AND COALESCE(cs.barcode,'') = COALESCE(UPPER(cend.barcode), '')
    AND cs.oven_hardware_id = cend.deviceid
    AND cs.cook_start_time < cend.time_stamp
    AND TIMESTAMPDIFF('hours', cs.cook_start_time, cend.time_stamp) < 24
  LEFT JOIN {{ ref('user_activity_statuses') }} uas
    ON cs.customer_id = uas.customer_id
    AND cs.cook_start_time >= uas.status_start_time
    AND cs.cook_start_time < COALESCE(uas.status_end_time,'9999-12-31')
  WHERE cs.oven_generation = 'gen_1'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
)
(SELECT * FROM gen2_and_airvala_ovens)
UNION 
(SELECT * FROM gen1_ovens)
