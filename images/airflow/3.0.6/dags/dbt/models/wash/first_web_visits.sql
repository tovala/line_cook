
SELECT
  fs.customer_id
  , pv.ip_address
  , pv.page_visit_id
  , pv.page_visit_start_time
  , pv.session_id
  , pv.page_url
  , pv.device
  , pv.operating_system
  , pv.page_utm_campaign
  , lp.channel
  , lp.channel_category
FROM {{ table_reference('page_visits', 'grind') }} pv
LEFT JOIN {{ table_reference('fullstory_session_to_customer', 'wash') }} fs 
  ON pv.session_id = fs.session_id
LEFT JOIN {{ ref('landing_pages') }} lp
  ON pv.session_id = lp.session_id
-- Adding customer_id to page_visit on same session_id
WHERE COALESCE(fs.customer_id, -1) NOT IN (
  SELECT 
    lead_id
  FROM {{ ref('leads') }}
  WHERE registration_time < (
    SELECT 
      MIN(session_start_time) 
    FROM {{ ref('sessions') }}
  )
)
-- Excluding customers who registered before fullstory data.
-- Not all visitors have customer_id, so this create a placeholder for all the nulls.
QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(fs.customer_id::STRING, pv.ip_address) ORDER BY pv.page_visit_start_time ) = 1
-- For each customer_id find the earliest web page_visit,then for each IP_address find the earliest web page_visit.
-- Assuming each IP address is individual visitor.
