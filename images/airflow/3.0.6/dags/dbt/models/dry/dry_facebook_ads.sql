{{
  config(
    alias='facebook_ads',
    materialized='incremental', 
    unique_key="id",
    tags=['online_spend']
  ) 
}}

-- Conversions are intentionally not pulled in with the Facebook Ads data right now 
-- Facebook's API deprecated access to the offsite conversion event we were using in May 2025 
-- If it becomes business-critical, we can try to figure out an alternate solution to pull CVR 

SELECT 
    _airbyte_raw_id
    , {{ hash_natural_key('ad_id', 'adset_id', 'campaign_id', 'date_start') }} AS id 
    , ad_id 
    , ad_name 
    , adset_id 
    , adset_name
    , campaign_id 
    , campaign_name 
    , date_start AS report_date
    , attribution_setting 
    , objective AS campaign_objective 
    , spend
    , clicks 
    , unique_clicks 
    , outbound_clicks AS outbound_clicks_array
    , impressions 
    , _airbyte_extracted_at
FROM {{ source('facebook_ads', 'ads_insights') }} 
{% if is_incremental() %}
  WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}} )
{%- endif -%}
