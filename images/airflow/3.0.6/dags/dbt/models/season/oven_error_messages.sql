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
  , oe.event_time AS error_time
  , oe.local_event_time AS local_error_time
  , oe.serial_number
  , oe.customer_id
  , oe.oven_registration_id
  , ec.class::STRING AS error_group
  , CASE WHEN error_group = '2'
         THEN ec.error::STRING 
         ELSE REGEXP_SUBSTR(ec.error, '-\\d')::STRING 
    END AS error_subgroup
  , error_group || error_subgroup AS error_code
  , error_code IN ('2-1', '2-23', '3-1', '3-2', '3-3', '3-4', '3-5', '3-6', '3-7', '3-8', '3-9', '8-1', '8-2') 
                   OR error_code LIKE '5-%' AS is_valid_code
  -- ORIGINAL DEPRECATED SOURCE: https://tovala.atlassian.net/wiki/spaces/Mini/pages/388792327/Mini+error+codes
  -- Updated source from Shawn L: https://docs.google.com/spreadsheets/d/1cLk77bMBlyMFXsHjYI6aTKUlidNC1HuSlnFc-60Qc3s/edit#gid=1981415192
  , CASE WHEN error_code = '2-1'
         THEN 'Serial number mismatch.'
         WHEN error_code = '2-23'
         THEN 'Error starting preset chef.'
         WHEN error_code = '3-1'
         THEN 'Chamber temperature high.'
         WHEN error_code = '3-2'
         THEN 'Chamber temperature low.'
         WHEN error_code = '3-3'
         THEN 'Chamber temperature forced error.'
         WHEN error_code = '3-4'
         THEN 'Wall temperature high.'
         WHEN error_code = '3-5'
         THEN 'Wall temperature low.'
         WHEN error_code = '3-6'
         THEN 'Wall temperature forced error.'
         WHEN error_code = '3-7'
         THEN 'Temperature calculation out of bounds'
         WHEN error_code IN ('3-8', '3-9')
         THEN 'Other temperature error.'
         WHEN error_code LIKE '5-%'
         THEN 'Forced error.'
         WHEN error_code = '8-1'
         THEN 'Cook cycle improperly formatted.'
         WHEN error_code = '8-2'
         THEN 'Bad routine response from API.'
    END AS error_message
  , oe.updated
FROM {{ table_reference('oven_events', 'grind') }} oe 
LEFT JOIN {{ table_reference('oven_logs_errorcode') }} ec 
  ON oe.oven_event_id = ec.oven_event_id
WHERE oe.key = 'errorCode'
  {% if is_incremental() %}
  AND oe.updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}