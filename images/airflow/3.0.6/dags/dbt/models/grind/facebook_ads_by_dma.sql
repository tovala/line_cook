{{
  config(
    materialized='incremental', 
    unique_key='id',
    tags=["supermetrics", "thyme_incremental"]
  ) 
}}
WITH fb AS {{facebook_ads_name_json_cte('supermetrics', 'fbads_facebook_ads_by_dma')}}
SELECT 
  {{ hash_natural_key('TRY_TO_NUMERIC(fb.ad_id)::INT', 'TRY_TO_NUMERIC(fb.campaign_id)::INT', 'fb.date', 'fb.dma') }} AS id
  , TRY_TO_NUMERIC(fb.profile_id)::INT AS profile_id
  , fb.profile AS profile_name
  , TRY_TO_NUMERIC(fb.ad_id)::INT AS ad_id
  , fb.ad_name
  , TRY_TO_NUMERIC(fb.campaign_id)::INT AS campaign_id
  , fb.campaign_name
  , TRY_TO_NUMERIC(fb.ad_group_id)::INT AS ad_group_id
  , fb.ad_group_name
  , {{ array_string_to_variant('fb.campaign_tags') }} AS campaign_tags
  , fb.date AS report_date
  , CASE WHEN fb.dma = 'Cedar Rapids-Wtrlo-Iwc&Dub'
         THEN 'Cedar Rapids-Wtrlo-Iwc &Dub'
         WHEN fb.dma = 'Washington, DC (Hagrstwn)'
         THEN 'Washington, DC (Hagerstown)'
         WHEN fb.dma = 'Idaho Fals-Pocatllo(Jcksn)'
         THEN 'Idaho Falls - Pocatello'
         WHEN fb.dma = 'Sioux Falls(Mitchell)'
         THEN 'Sioux Falls (Mitchell)'
         WHEN fb.dma = 'Rochester-Mason City-Austin'
         THEN 'Rochestr-Mason City-Austin' 
         ELSE fb.dma 
    END AS dma_name_fb
  , CASE WHEN LOWER(dma_name_fb) = 'unknown' 
         THEN dma_name_fb 
         ELSE map.dma_description 
    END AS dma
  , COALESCE(fb.action_comment, 0) AS post_comments
  , COALESCE(fb.action_post_like, 0) AS post_likes
  , COALESCE(fb.action_link_click, 0) AS link_clicks
  , COALESCE(fb.clicks, 0) AS all_clicks
  , COALESCE(fb.outbound_clicks, 0) AS outbound_clicks
  , COALESCE(fb.cost, 0) AS spend
  , COALESCE(fb.impressions, 0) AS impressions
  , COALESCE(fb.reach, 0) AS reach
  , COALESCE(fb.action_video_view, 0) AS three_second_video_views
  , ad_name_json:at::STRING AS asset_type
  , ad_name_json:lg::STRING AS ad_length
  , ad_name_json:hk::STRING AS hook
  , ad_name_json:of::STRING AS ad_offer
  , ad_name_json:pr::STRING AS price
  , ad_name_json:an::STRING AS asset_name
  , ad_name_json:id::STRING AS jump_unique_id
  , ad_name_json:lp::STRING AS landing_page
  , ad_name_json:bo::STRING AS body_copy_theme
  , ad_name_json:ca::STRING AS call_to_action
  , ad_name_json:me::STRING AS meal
  , ad_name_json:c1::STRING AS c1
  , ad_name_json:ki::STRING AS key_image
  , ad_name_json:sp::STRING AS speaker
  , ad_name_json:re::STRING AS resolution
FROM fb
LEFT JOIN {{ source('brine', 'dma_name_mapping') }} map 
  ON dma_name_fb = map.dma_name_facebook
{{ load_incrementally_supermetrics(30) }}