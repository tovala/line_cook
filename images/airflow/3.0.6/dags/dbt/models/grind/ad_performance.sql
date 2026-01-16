
(SELECT 
   sta.id 
   , 'tiktok' AS data_source
   , 'TikTok' AS channel_name
   , sta.ad_group_id
   , sta.ad_group_name
   , sta.ad_id
   , ARRAY_CONSTRUCT() AS ad_labels
   , sta.ad_name
   , sta.campaign_id
   , sta.campaign_name
   , sta.campaign_objective_type AS campaign_objective
   , sta.report_date AS spend_date
   , NULL AS profile_name
   , NULL AS profile_id
   , COALESCE(sta.clicks, 0) AS clicks
   , sta.cost AS spend
   , COALESCE(sta.impressions, 0) AS impressions
   , sta.complete_payment AS oven_sales
 FROM {{ table_reference('supermetrics_tiktok_ads') }} sta
 )
UNION
(SELECT 
   sfa.id 
   , 'facebook' AS data_source
   , 'Facebook / Instagram' AS channel_name
   , sfa.ad_group_id
   , sfa.ad_group_name
   , sfa.ad_id
   , ARRAY_CONSTRUCT() AS ad_labels
   , sfa.ad_name
   , sfa.campaign_id
   , sfa.campaign_name
   , sfa.campaign_objective
   , sfa.report_date AS spend_date
   , sfa.profile_name
   , sfa.profile_id
   , COALESCE(sfa.clicks, 0) AS clicks
   , sfa.cost AS spend
   , COALESCE(sfa.impressions, 0) AS impressions
   , COALESCE(sfa.offsite_conversions_fb_pixel_purchase, 0) AS oven_sales
 FROM {{ table_reference('supermetrics_facebook_ads') }} sfa
)
UNION
(SELECT 
   sga.id 
   , 'google' AS data_source
   , gmap.channel_name
   , sga.ad_group_id
   , sga.ad_group_name
   , sga.ad_id
   , sga.ad_labels AS ad_labels
   , NULL AS ad_name
   , sga.campaign_id
   , sga.campaign_name
   , NULL AS campaign_objective
   , sga.report_date AS spend_date
   , sga.profile_name
   , sga.profile_id
   , COALESCE(sga.clicks, 0) AS clicks
   , sga.cost AS spend
   , COALESCE(sga.impressions, 0) AS impressions
   , sga.conversions AS oven_sales
 FROM {{ table_reference('supermetrics_google_ads') }} sga
 LEFT JOIN {{ ref('google_ad_to_channel_mapping') }} gmap 
   ON sga.id = gmap.id 
)
