{{
  config(
    materialized='incremental', 
    unique_key='page_visit_id',
    tags=["thyme_incremental"]
  ) 
}}

  SELECT 
    fe.row_id AS page_visit_id
    -- Event sequences are used to join events onto a page 
    , fe.event_sequence
    , LEAD(fe.event_sequence) OVER (PARTITION BY fe.sessionid ORDER BY fe.event_sequence) AS next_page_visit_event_sequence
    , fe.eventstart AS page_visit_start_time
    , COALESCE(LEAD(fe.eventstart) OVER (PARTITION BY fe.sessionid ORDER BY fe.event_sequence), ses.session_end_time) AS page_visit_end_time
    , TIMESTAMPDIFF('seconds', page_visit_start_time, page_visit_end_time) AS page_visit_duration_seconds
    , LAG(fe.event_sequence) OVER (PARTITION BY fe.sessionid ORDER BY fe.event_sequence) IS NULL AS is_first_in_session
    , next_page_visit_event_sequence IS NULL AS is_last_in_session
    , fe.sessionid AS session_id
    , LOWER(fe.pageurl) AS page_url
    , {{ identify_checkout_page('fe.pageurl') }} AS is_checkout_page
    , PARSE_URL(page_url) AS page_url_json 
    , page_url_json:"parameters":"utm_source"::STRING AS page_utm_source
    , page_url_json:"parameters":"utm_medium"::STRING AS page_utm_medium
    , page_url_json:"parameters":"utm_campaign"::STRING AS page_utm_campaign
    , page_url_json:"parameters":"utm_content"::STRING AS page_utm_content
    , page_url_json:"parameters":"utm_term"::STRING AS page_utm_term
    , TRY_TO_NUMERIC(page_url_json:"parameters":"utm_aid"::STRING) AS page_utm_ad_id
    , TRY_TO_NUMERIC(page_url_json:"parameters":"utm_adsetid"::STRING) AS page_utm_adset_id
    , page_url_json:"path"::STRING AS base_path
    , REGEXP_SUBSTR(page_url, 'https://[a-z0-9./-]+') AS base_url
    , REGEXP_REPLACE(REGEXP_SUBSTR(SPLIT_PART(page_url, '.com', 1) , '//[a-z0-9.-]+'), '//|www.', '') AS base_page
    , LOWER(fe.pagerefererurl) AS referer_url 
    , REGEXP_SUBSTR(referer_url, 'https://[a-z0-9./-]+') AS base_referer_url
    , REGEXP_REPLACE(REGEXP_SUBSTR(SPLIT_PART(referer_url, '.com', 1) , '//[a-z0-9.-]+'), '//|www.', '') AS base_referer_page
    , fe.pageagent AS agent
    , REPLACE(LOWER(fe.pagebrowser), ' ', '_') AS browser
    , LOWER(fe.pagedevice) AS device
    , REPLACE(LOWER(fe.pageoperatingsystem), ' ', '_') AS operating_system   
    , fe.pageip AS ip_address
    , SPLIT_PART(fe.pagelatlong, ',', 1)::FLOAT AS latitude
    , SPLIT_PART(fe.pagelatlong, ',', 2)::FLOAT AS longitude
  FROM {{ table_reference('fullstory_events')}} fe
  LEFT JOIN {{ ref('sessions') }} ses 
    ON fe.sessionid = ses.session_id
  WHERE fe.eventtype = 'navigate'
    AND fe.pageurl IS NOT NULL
    AND fe.pageurl LIKE 'https://%'
    {% if is_incremental() %}
    AND fe.sessionid IN (SELECT DISTINCT sessionid 
                        FROM {{ table_reference('fullstory_events')}}
                        WHERE eventstart >= 
                        (SELECT DATEADD('hour', -25, MAX(page_visit_start_time)) FROM {{this}} ))
    {%- endif -%}
