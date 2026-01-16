{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='cook_event_id'
    ) 
}}

WITH cook_start AS (
  SELECT 
    oe.oven_event_id AS cook_event_id
    , oe.updated
    , cs.cookcycleid AS cook_cycle_id
    , cs.unique_cook_cycle_id
    , oe.oven_hardware_id
    , oe.oven_generation
    , oe.hardware_group_name
    , oe.serial_number
    , oe.is_test_oven
    , oe.is_internal_account
    , oe.customer_id
    , oe.session_id
    , oe.event_time AS cook_start_time
    , cs.hwtimestamp AS hardware_start_time
    , oe.local_event_time AS local_cook_start_time
    , DATE_PART('hour', local_cook_start_time) AS local_hour_of_day
    , oe.event_source
    , oe.key
    {{ cook_event_subtypes() }}
    , UPPER(cs.barcode) AS barcode
    , LOWER(cs.type) AS preset_or_steadystate
    , CASE WHEN cs.mode IN ('air_fry', 'airfry')
           THEN 'airfry'
           WHEN cs.mode IN ('broil_hi', 'broil_high')
           THEN 'broil_high'
           WHEN cs.mode IN ('conv_bake', 'convection_bake')
           THEN 'convection_bake'
           WHEN cs.mode IN ('conv_steam', 'convection_steam')
           THEN 'convection_steam'
           ELSE cs.mode 
      END AS steadystate_type
    , cs.setduration AS initial_duration 
    , cs.settemperature AS initial_temperature
    , cs.routine AS cook_routine
    , oe.event_timezone
  FROM {{ table_reference('oven_events', 'grind') }} oe
  INNER JOIN {{ table_reference('oven_logs_cookingstarted') }} cs
    ON oe.oven_event_id = cs.oven_event_id
  LEFT JOIN {{ ref('tovala_assist_barcodes') }} tab 
    ON UPPER(cs.barcode) = tab.barcode
  LEFT JOIN {{ ref('tovala_preset_barcodes') }} tpb 
    ON UPPER(cs.barcode) = tpb.barcode
  LEFT JOIN {{ ref('user_defined_meals') }} udm
    ON UPPER(cs.barcode) = udm.alternate_barcode
    AND oe.customer_id = udm.customer_id
    AND oe.event_time >= udm.alternate_start_time
    AND oe.event_time < udm.alternate_end_time
  WHERE oe.key = 'cookingStarted'
    {% if is_incremental() %}
    AND cs.updated >= (SELECT MAX(updated) FROM {{ this }})
    AND NOT cs.oven_event_id IN (SELECT cook_event_id FROM {{ this }})
    {% endif %}
    AND COALESCE(UPPER(cs.barcode), '') NOT LIKE 'BANG%'
    AND COALESCE(UPPER(cs.barcode), '') NOT IN ('CUSTOMER_SUPPORT-CONVECTION_FAN_TEST', 
                                                'THE_TEST_BARCODE_1', 
                                                'THE_TEST_BARCODE_2', 
                                                'TEST-NEW-STYLE')
), cook_routines_parsed AS (
  SELECT 
    unique_cook_cycle_id 
    , BOOLOR_AGG(step_mode = 'test') AS is_test_event
    , SUM(step_time) AS cook_routine_duration
    , BOOLOR_AGG(step_mode IN ('broil_high', 'broil_low')) AS uses_broil
    , BOOLOR_AGG(step_mode = 'steam') AS uses_steam
  FROM {{ table_reference('cook_event_steps', 'wash') }}
  GROUP BY 1
)
SELECT 
  cs.cook_event_id
  , cs.updated
  , cs.cook_cycle_id
  , cs.unique_cook_cycle_id
  , cs.oven_hardware_id
  , cs.oven_generation
  , cs.hardware_group_name
  , cs.serial_number
  , cs.is_test_oven
  , cs.is_internal_account
  , cs.customer_id
  , cs.session_id
  , cs.cook_start_time
  , cs.hardware_start_time
  , cs.local_cook_start_time
  , CASE WHEN cs.local_hour_of_day >= 5 AND cs.local_hour_of_day < 7
         THEN 'early_breakfast'
         WHEN cs.local_hour_of_day >= 7 AND cs.local_hour_of_day < 9
         THEN 'avg_breakfast'
         WHEN cs.local_hour_of_day >= 9 AND cs.local_hour_of_day < 11
         THEN 'late_breakfast'
         WHEN cs.local_hour_of_day >= 11 AND cs.local_hour_of_day < 12
         THEN 'early_lunch'
         WHEN cs.local_hour_of_day >= 12 AND cs.local_hour_of_day < 13
         THEN 'avg_lunch'
         WHEN cs.local_hour_of_day >= 13 AND cs.local_hour_of_day < 15
         THEN 'late_lunch'
         WHEN cs.local_hour_of_day >= 15 AND cs.local_hour_of_day < 17
         THEN 'afternoon_snack'
         WHEN cs.local_hour_of_day >= 17 AND cs.local_hour_of_day < 18
         THEN 'early_dinner' 
         WHEN cs.local_hour_of_day >= 18 AND cs.local_hour_of_day < 20
         THEN 'avg_dinner' 
         WHEN cs.local_hour_of_day >= 20 AND cs.local_hour_of_day < 23
         THEN 'late_dinner' 
         WHEN cs.local_hour_of_day = 23 OR (cs.local_hour_of_day >= 0 AND cs.local_hour_of_day < 5)
         THEN 'midnight_snack'
    END AS time_of_meal_category 
  , TIMESTAMPADD('seconds', crp.cook_routine_duration, cs.cook_start_time) AS expected_end_time
  , cs.event_source
  , cs.key
  , cs.cook_event_subtype
  , cs.barcode
  , cs.preset_or_steadystate
  , cs.steadystate_type
  , cs.initial_duration 
  , cs.initial_temperature
  , cs.cook_routine
  , crp.cook_routine_duration
  , COALESCE(crp.uses_broil, FALSE) AS uses_broil 
  , COALESCE(crp.uses_steam, FALSE) AS uses_steam
  -- TODO: extend this definition to gen_1 barcodes
  , COALESCE(crp.is_test_event, FALSE) OR COALESCE(RLIKE(cs.barcode, '.*(TEST|DEBUG:).*', 'i'), FALSE) AS is_test_event 
  , cs.event_timezone
FROM cook_start cs 
LEFT JOIN cook_routines_parsed crp 
  ON cs.unique_cook_cycle_id = crp.unique_cook_cycle_id
