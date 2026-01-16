{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='oven_event_id'
    ) 
}}

SELECT
  olc.oven_event_id 
  , olc.agentid AS agent_id 
  , olc.devicegroupid AS hardware_group_id
  , olc.devicegroupname AS hardware_group_name
  , CASE WHEN RLIKE(hardware_group_name, '^20\\d{2}-\\d{2}_Production(.*?)') 
         THEN REGEXP_SUBSTR(hardware_group_name, '^20\\d{2}-\\d{2}') 
    END AS hardware_group_date
  , olc.deviceid AS oven_hardware_id
  , olc.key
  , olc.sessionid AS session_id 
  , olc.source AS event_source
  , olc.time_stamp AS event_time
  , CONVERT_TIMEZONE(hz.timezone, olc.time_stamp) AS local_event_time
  , olc.updated
  , di.oven_generation
  , di.serial_number 
  , di.build_id
  , di.batch_number
  , roh.customer_id 
  , roh.oven_registration_id
  , di.is_oven_registered
  , roh.is_most_recent_record
  , di.first_oven_registration_time
  , roh.oven_name
  , di.current_hardware_group
  -- According to Adam, non-test ovens should always start with YYYY-MM_Production
  , COALESCE(hardware_group_name LIKE '%|%' OR NOT REGEXP_LIKE(hardware_group_name, '^\\d{4}-\\d{2}_Production.*'), FALSE) AS is_test_oven
  , COALESCE(roh.is_internal_account, FALSE) AS is_internal_account
  , COALESCE(di.is_basketvala, FALSE) AS is_basketvala
  , di.blessed_at_time
  , hz.zip_cd AS event_zip_cd
  , hz.timezone AS event_timezone
FROM {{ source('kinesis', 'oven_logs_combined') }} olc
INNER JOIN {{ ref('ovens') }} di 
  ON olc.deviceid = di.oven_hardware_id
LEFT JOIN {{ ref('historic_oven_registrations') }} roh 
  ON olc.deviceid = roh.oven_hardware_id 
  AND ((olc.time_stamp >= roh.start_time AND olc.time_stamp < COALESCE(roh.end_time, '9999-01-01'))
      OR roh.row_num = 1 AND roh.start_time > olc.time_stamp)
LEFT JOIN {{ ref('historic_zips') }} hz
  ON roh.customer_id = hz.customer_id 
  AND ((olc.time_stamp >= hz.zip_start_time AND olc.time_stamp < hz.zip_end_time)
        OR (hz.is_first_row AND olc.time_stamp < hz.zip_start_time))
-- NOTE: if you add another key, you will need to fully refresh this table
WHERE olc.key IN 
  ('bootup-complete',
   'cookingStarted',
   'errorCode',
   'global_exception',
   'huge_allocation',
   'temperature')
  {% if is_incremental() %}
  AND updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}