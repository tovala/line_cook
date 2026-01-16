{{
  config(
    materialized='incremental', 
    unique_key='id',
    tags=["supermetrics", "thyme_incremental"]
  ) 
}}
WITH fabs AS {{ facebook_ads_name_json_cte('supermetrics', 'fbads_facebook_ads_by_state')}}
SELECT 
  {{ hash_natural_key('TRY_TO_NUMERIC(fabs.ad_id)::INT', 'TRY_TO_NUMERIC(fabs.campaign_id)::INT', 'fabs.date', 'fabs.region') }} AS id
  , TRY_TO_NUMERIC(fabs.profile_id)::INT AS profile_id
  , fabs.profile AS profile_name
  , TRY_TO_NUMERIC(fabs.ad_id)::INT AS ad_id
  , fabs.ad_name
  , TRY_TO_NUMERIC(fabs.campaign_id)::INT AS campaign_id
  , fabs.campaign_name
  , TRY_TO_NUMERIC(fabs.ad_group_id)::INT AS ad_group_id
  , fabs.ad_group_name
  , {{ array_string_to_variant('fabs.campaign_tags') }} AS campaign_tags
  , fabs.date AS report_date
  , fabs.region AS state
  , CASE WHEN fabs.region = 'Washington, District of Columbia' 
               THEN 'DC' 
               ELSE st.state_abbreviation 
    END AS state_abbreviation
  , COALESCE(fabs.action_comment, 0) AS post_comments
  , COALESCE(fabs.action_post_like, 0) AS post_likes
  , COALESCE(fabs.action_link_click, 0) AS link_clicks
  , COALESCE(fabs.clicks, 0) AS all_clicks
  , COALESCE(fabs.outbound_clicks, 0) AS outbound_clicks
  , COALESCE(fabs.cost, 0) AS spend
  , COALESCE(fabs.impressions, 0) AS impressions
  , COALESCE(fabs.reach, 0) AS reach
  , COALESCE(fabs.action_video_view, 0) AS three_second_video_views
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
FROM fabs
LEFT JOIN {{ source('brine', 'state_timezones')}} st
  ON fabs.region = st.state_name
{{ load_incrementally_supermetrics(30) }}