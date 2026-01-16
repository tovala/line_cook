{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='oven_event_id'
    ) 
}}

-- Gen2 only
SELECT 
  oe.oven_event_id
  , oe.updated
  , oe.agent_id
  , oe.hardware_group_id
  , oe.hardware_group_name
  , oe.build_id
  , oe.oven_hardware_id
  , oe.session_id
  , oe.event_time AS temperature_time
  , oe.local_event_time AS local_temperature_time
  , oe.serial_number
  , oe.customer_id
  , oe.oven_registration_id
  , olt.cookcycleid AS cook_cycle_id
  , olt.unique_cook_cycle_id
  , olt.chambertemperature AS chamber_temperature
  , olt.walltemperature AS wall_temperature
  , CASE WHEN oe.hardware_group_date IN ('2018-10', '2019-03', '2019-06', '2019-07', '2019-09')
         THEN .25 
         WHEN oe.hardware_group_date = '2019-11'
         THEN .9
         WHEN oe.hardware_group_date = '2020-02' AND oe.build_id = 'TOVMN2018'
         THEN 1.27
         WHEN oe.hardware_group_date = '2020-02' AND oe.build_id = 'TOVMN2015'
         THEN 1.38
         WHEN oe.hardware_group_date = '2020-02'
         THEN 1.2
    END AS calibration_constant
  -- See DEA-744 for details on temperature probe update
  , CASE WHEN olt.barrelTemperature IS NOT NULL 
         THEN olt.barrelTemperature
         WHEN ((oe.hardware_group_name = '2019-07_Production 1' AND temperature_time > '2021-06-25 17:02:01')
               OR (oe.hardware_group_name = '2019-07_Production 2' AND temperature_time > '2021-06-28 17:36:58')
               OR (oe.hardware_group_name = '2019-09_Production 1' AND temperature_time > '2021-06-25 17:04:46')
               OR (oe.hardware_group_name = '2019-09_Production 2' AND temperature_time > '2021-06-25 17:49:50')
               OR (oe.hardware_group_name = '2019-11_Production' AND temperature_time > '2021-06-28 17:44:52')
               OR (oe.build_id = 'TOVMN2027' AND temperature_time > '2021-06-29 06:13:25')
               OR (oe.hardware_group_name = '2020-02_Production' AND 
                    ((oe.build_id = 'TOVMN2007' AND temperature_time > '2021-07-07 16:22:01') OR
                     (oe.build_id = 'TOVMN2011' AND temperature_time > '2021-07-09 00:39:00') OR
                     (oe.build_id = 'TOVMN2013' AND temperature_time > '2021-07-11 22:07:07') OR
                     (oe.build_id = 'TOVMN2015' AND temperature_time > '2021-07-07 16:35:48') OR
                     (oe.build_id = 'TOVMN2016' AND temperature_time > '2021-07-07 17:53:19'))))
         THEN (olt.chamberTemperature + 23.053)/1.289
         ELSE (olt.chambertemperature + (calibration_constant * olt.walltemperature))/(calibration_constant+1) 
    END AS barrel_temperature
  , DATEDIFF('seconds', cec.cook_start_time, oe.event_time) AS time_since_cook_start_seconds
FROM {{ table_reference('oven_events', 'grind') }} oe 
INNER JOIN {{ table_reference('oven_logs_temperature') }} olt 
  ON oe.oven_event_id = olt.oven_event_id
INNER JOIN {{ ref('cook_events') }} cec 
  ON olt.unique_cook_cycle_id = cec.unique_cook_cycle_id
WHERE olt.source IN ('mini', 'airvala', 'gen2')
  {% if is_incremental() %}
  AND oe.updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
 