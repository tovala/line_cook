{{
  config(
    materialized='incremental', 
    unique_key='id',
    tags=["supermetrics", "thyme_incremental"]
  ) 
}}

WITH fb_placement AS {{ facebook_ads_name_json_cte('supermetrics', 'fbads_facebook_ads_by_placement')}}
SELECT 
  {{ hash_natural_key('TRY_TO_NUMERIC(ad_id)::INT', 'TRY_TO_NUMERIC(campaign_id)::INT', 'date', 'device_platform','platform_position','impression_device','publisher_platform') }} AS id
  , TRY_TO_NUMERIC(profile_id)::INT AS profile_id
  , profile AS profile_name
  , TRY_TO_NUMERIC(ad_id)::INT AS ad_id
  , ad_name
  , TRY_TO_NUMERIC(campaign_id)::INT AS campaign_id
  , campaign_name
  , TRY_TO_NUMERIC(ad_group_id)::INT AS ad_group_id
  , ad_group_name
  , {{ array_string_to_variant('campaign_tags') }} AS campaign_tags
  , creative_body
  , date AS report_date
  , device_platform
  , impression_device
  , platform_position
  , publisher_platform
  , COALESCE(action_comment, 0) AS post_comments
  , COALESCE(action_post_like, 0) AS post_likes
  , COALESCE(action_link_click, 0) AS link_clicks
  , COALESCE(clicks, 0) AS all_clicks
  , COALESCE(outbound_clicks, 0) AS outbound_clicks
  , COALESCE(cost, 0) AS spend
  , COALESCE(impressions, 0) AS impressions
  , COALESCE(reach, 0) AS reach
  , COALESCE(landing_page_views, 0) AS landing_page_views
  , COALESCE(offsite_conversions_fb_pixel_complete_registration, 0) AS complete_registrations
  , COALESCE(offsite_conversions_fb_pixel_purchase, 0) AS oven_sales
  , COALESCE(action_video_view, 0) AS three_second_video_views
  , COALESCE(video_average_watch_time, 0) AS video_average_watch_time
  , COALESCE(video_p_25_watched_actions, 0) AS video_25p_views
  , COALESCE(video_p_50_watched_actions, 0) AS video_50p_views
  , COALESCE(video_p_75_watched_actions, 0) AS video_75p_views
  , COALESCE(video_p_100_watched_actions, 0) AS video_100p_views
  , COALESCE(video_thruplay_watched_actions, 0) AS thruplay_video_views
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
FROM fb_placement
{{ load_incrementally_supermetrics(30) }}