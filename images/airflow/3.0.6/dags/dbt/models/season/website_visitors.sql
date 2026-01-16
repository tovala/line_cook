
SELECT 
  s.fullstory_user_id
  , s.session_start_time AS first_site_visit_time
  , lp.channel AS first_channel
  , lp.channel_category AS first_channel_category
  , lp.landing_page AS first_landing_page
  , lp.landing_path AS first_landing_path
  , lp.marketing_ad_id AS first_marketing_ad_id
  , lp.marketing_campaign AS first_marketing_campaign
  , s.session_id AS first_session_id
  , lp.session_id IS NULL AS was_first_session_in_app
FROM {{ ref('sessions') }} s
LEFT JOIN {{ ref('landing_pages') }} lp
  ON s.session_id = lp.session_id
WHERE s.session_number = 1
