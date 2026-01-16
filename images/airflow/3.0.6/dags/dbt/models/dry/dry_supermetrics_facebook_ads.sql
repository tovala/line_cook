{{
  config(
    alias='supermetrics_facebook_ads',
    materialized='incremental', 
    unique_key='id',
    tags=['supermetrics']
  ) 
}}

SELECT 
  -- There is no primary key in supermetrics, this is a hash of all of the dimensional IDs and the date
  {{ hash_natural_key('profile_id::INT', 'ad_id::INT', 'campaign_id::INT', 'ad_group_id::INT', 'date') }} AS id
  , profile_id::INT AS profile_id
  , profile AS profile_name
  , ad_id::INT AS ad_id
  , ad_name
  , campaign_id::INT AS campaign_id
  , campaign_name
  , ad_group_id::INT AS ad_group_id
  , ad_group_name
  , {{ array_string_to_variant('campaign_tags') }} AS campaign_tags
  , campaign_objective
  , date AS report_date
  , action_link_click
  , clicks
  , cost
  , impressions
  , offsite_conversions_fb_pixel_purchase
  , outbound_clicks
FROM {{ source('supermetrics', 'fbads_facebook_ads') }}
{{ load_incrementally_supermetrics(30) }}
