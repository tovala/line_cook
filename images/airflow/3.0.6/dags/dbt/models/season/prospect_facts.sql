
SELECT 
  l.lead_id
  , cf.first_interaction_time
  , DATEDIFF('day', l.registration_time, cf.first_interaction_time) AS days_as_prospect
  , COUNT(DISTINCT s.session_id) AS session_count
  , MIN(s.session_start_time) AS first_session_start_time
  , MAX(s.session_start_time) AS latest_session_start_time
  --Current checkout page is buy.tovala.com, pre-Oct-22 it was my.tovala.com/start
  , MIN(CASE WHEN pv.is_checkout_page THEN pv.page_visit_start_time END) AS first_checkout_visit_time
  , first_checkout_visit_time IS NOT NULL AS has_visited_checkout
  , COUNT(DISTINCT CASE WHEN pv.is_checkout_page THEN pv.session_id END) AS checkout_visit_count
  --These can be null if no page visits occured during the visitors session
  , MIN_BY(lp.channel, s.session_start_time) AS first_touch_channel
  , MIN_BY(lp.channel_category, s.session_start_time) AS first_touch_channel_category
  , MAX_BY(lp.channel, s.session_start_time) AS latest_channel
  , MAX_BY(lp.channel_category, s.session_start_time) AS latest_channel_category
FROM {{ ref ('leads') }} l
LEFT JOIN {{ ref('customer_facts') }} cf
  ON l.lead_id = cf.customer_id
--All pages or sessions prior to becoming a customer. If there is no customer time yet, we still want to keep counting.
LEFT JOIN {{ ref('sessions') }} s
  ON l.lead_id = s.customer_id
  AND s.session_start_time <= COALESCE(cf.first_interaction_time, '9999-12-31')
LEFT JOIN {{ table_reference('page_visits', 'grind') }} pv
  ON s.session_id = pv.session_id
  --Separate statement for page visits since some sessions do not contain any page visits
  --Also you can visit more pages within the same session as purchase which we do not want to count
  AND pv.page_visit_start_time <= COALESCE(cf.first_interaction_time, '9999-12-31')
LEFT JOIN {{ ref('landing_pages') }} lp
  ON s.session_id = lp.session_id
GROUP BY 1,2,3
