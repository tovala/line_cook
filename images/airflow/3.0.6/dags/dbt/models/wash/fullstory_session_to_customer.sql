{{ 
  config(
    materialized='incremental',
    unique_key='session_customer_id',
    tags=["thyme_incremental"]
  )
}}
-- TODO: determine if this is the best way to dedupe the relationship between fullstory_user_id and customer_id - i.e. use the most recent row 
WITH fullstory_customers AS (
  SELECT 
    indvid AS fullstory_user_id
    , userappkey::INTEGER AS customer_id
  FROM {{ table_reference('fullstory_events') }} 
  WHERE userappkey IS NOT NULL
    AND RLIKE(userappkey, '\\d+')
    {% if is_incremental() %}
    AND updated > (SELECT DATEADD('hour', -48, MAX(updated)) FROM {{this}} )
    {%- endif -%}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY fullstory_user_id ORDER BY eventstart DESC, event_sequence DESC) = 1
), fullstory_sessions AS (
  SELECT  
    fe.sessionid AS session_id 
    , fe.indvid AS fullstory_user_id
    , MAX(CASE WHEN RLIKE(userappkey, '\\d+')
               THEN userappkey::INTEGER
          END) AS customer_id
  FROM {{ table_reference('fullstory_events') }} fe 
  {% if is_incremental() %}
  WHERE updated > (SELECT DATEADD('hour', -48, MAX(updated)) FROM {{this}} )
  {%- endif -%}
  GROUP BY 1,2
)
SELECT DISTINCT 
  {{ hash_natural_key('fs.session_id', 'COALESCE(fs.customer_id, fc.customer_id)') }} AS session_customer_id
  , fs.session_id 
  , COALESCE(fs.customer_id, fc.customer_id) AS customer_id
  , {{ current_timestamp_utc() }} AS updated
FROM fullstory_sessions fs 
LEFT JOIN fullstory_customers fc 
  ON fs.fullstory_user_id = fc.fullstory_user_id   
WHERE COALESCE(fs.customer_id, fc.customer_id) IS NOT NULL
