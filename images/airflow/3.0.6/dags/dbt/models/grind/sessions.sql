
SELECT 
  fe.sessionid AS session_id 
  , fstc.customer_id 
  -- Indvid identifies individual across sessions and devices, etc https://developer.fullstory.com/server/v1/segments/export-fields/
  , MAX_BY(fe.indvid, fe.eventstart) AS fullstory_user_id
  -- These can be NULL, the MAX removes the NULL 
  , MAX(fe.appname) AS app_name 
  , MAX(fe.apppackagename) AS app_package_name
  , MIN(fe.eventstart) AS session_start_time 
  , MAX(fe.eventstart) AS session_end_time
  , TIMESTAMPDIFF('seconds', session_start_time, session_end_time) AS session_duration_seconds
  , COUNT(fe.row_id) AS session_event_count
  , MIN_BY(fe.pagedevice, fe.eventstart) AS device_type
  , ROW_NUMBER() OVER (PARTITION BY fullstory_user_id ORDER BY session_start_time) AS session_number
  , CASE WHEN session_number = 1 THEN 'new' ELSE 'returning' END AS visitor_type
  --Remove null urls since you can navigate and not be linked to a webpage
  , COUNT(CASE WHEN fe.eventtype = 'navigate' AND fe.pageurl IS NOT NULL THEN fe.row_id END) AS webpage_count
  , COUNT(CASE WHEN fe.eventtype = 'navigate' AND fe.pageurl IS NOT NULL AND NOT {{ identify_checkout_page('fe.pageurl') }} THEN fe.row_id END) AS non_checkout_page_count
FROM {{ table_reference('fullstory_events') }} fe
LEFT JOIN {{ table_reference('fullstory_session_to_customer', 'wash') }} fstc 
  ON fe.sessionid = fstc.session_id
GROUP BY 1, 2
