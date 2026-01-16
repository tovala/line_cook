{{ 
    config(
        materialized='incremental',
        unique_key='row_id',
        tags=["thyme_incremental"]
    ) 
}}

WITH key_list AS (
  (SELECT key FROM {{ ref('app_analytics_keys') }})
    UNION
  (SELECT key FROM {{ ref('growth_analytics_keys') }})
     UNION 
  (SELECT deprecated_key AS key FROM {{ ref('growth_analytics_key_map') }})
)
SELECT
  fe.row_id
  , LAG(fe.row_id) OVER (PARTITION BY fe.sessionid ORDER BY fe.event_sequence) AS previous_event_row_id
  , LEAD(fe.row_id) OVER (PARTITION BY fe.sessionid ORDER BY fe.event_sequence) AS next_event_row_id
  , fe.sessionid AS session_id
  , fe.userid AS fullstory_user_id
  , fe.userdisplayname AS user_display_name
  , fe.useremail AS email
  , fe.updated AS updated_time
  , fe.event_sequence
  , CASE WHEN dep.key IS NOT NULL 
         THEN dep.key 
         ELSE fe.eventcustomname 
    END AS event_custom_name
  , fe.eventstart AS event_start_time
  , LEAD(fe.eventstart) OVER (PARTITION BY session_id ORDER BY event_sequence) AS event_end_time
  , LAG(fe.eventcustomname) OVER (PARTITION BY session_id ORDER BY event_sequence) AS previous_event_custom_name
  , LEAD(fe.eventcustomname) OVER (PARTITION BY session_id ORDER BY event_sequence) AS next_event_custom_name
  , fe.pagebrowser AS page_browser
  , fe.pagedevice AS page_device
  , fe.pageip AS page_ip
  , fe.pagelatlong AS page_latitude_longitude
  , fe.pageoperatingsystem AS page_os
  , fe.appname AS app_name
  , fe.evt_source_id_str AS source_click
  , fe.evt_question_str AS question_clicked
  , fe.evt_page_slug_str AS page_url
  , fe.evt_start_date AS term_start_date
  , fe.evt_term_id_str AS term_id
  , fe.evt_navigation_item_text_str AS navigation_item
  , fe.evt_button_text_str AS button_text
  , fe.evt_filter_id_str AS filter
  , fe.evt_meal_plan_count_str AS meal_plan
  , fe.evt_oven_type_str AS oven_generation
  , fe.evt_number_str AS clicked_number
  , fe.evt_zip_code_str AS zip_cd
FROM {{ table_reference('fullstory_events') }} fe
LEFT JOIN {{ ref('growth_analytics_key_map') }} dep 
  ON fe.eventcustomname = dep.deprecated_key
WHERE (fe.eventcustomname IN (SELECT key FROM key_list) 
  OR (fe.eventtype = 'navigate' AND fe.pageurl IS NOT NULL AND fe.pageurl LIKE 'https://%'))
{% if is_incremental() %}
  AND fe.updated > (SELECT DATEADD('hour', -48, MAX(updated_time)) FROM {{this}} )
{% endif %}
