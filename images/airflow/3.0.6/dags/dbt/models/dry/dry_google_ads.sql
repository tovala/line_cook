{{
  config(
    alias='google_ads',
    materialized='incremental', 
    unique_key="id", 
    tags=['online_spend']
  ) 
}}

-- In Google Ads, campaign labels identify which type of Google Ad is being utilized in a campaign 
-- We pull all of these per campaign to then classify the campaign below 
WITH campaign_labels AS (
    SELECT 
        "CAMPAIGN.ID"::INT AS campaign_id 
        , ARRAY_AGG("LABEL.NAME") AS label_name
    FROM {{ source('google_ads', 'campaign_label') }}
    GROUP BY ALL 
)
-- Each row is unique per Campaign ID / Report Date / Report Hour / Ad Network combination 
SELECT 
    {{ hash_natural_key('c."CAMPAIGN.ID"', 'c."SEGMENTS.DATE"', 'c."SEGMENTS.HOUR"', 'c."SEGMENTS.AD_NETWORK_TYPE"') }} AS id 
    , c."CAMPAIGN.ID"::INT AS campaign_id 
    , CASE WHEN "CAMPAIGN.NAME" ILIKE '%Non-Brand%' OR ARRAY_TO_STRING(cl.label_name, ',') ILIKE '%non brand%'  
          THEN false 
          WHEN "CAMPAIGN.NAME" ILIKE '%Brand%' 
          THEN true 
      END AS is_branded 
   , CASE WHEN "CAMPAIGN.NAME" ILIKE '%display%' OR "CAMPAIGN.NAME" IN ('2025_PM_DemandGen_Image_LDS','2025_PM_DemandGen_Image_Prospecting')
          THEN 'Display'
          WHEN "CAMPAIGN.NAME" ILIKE '%pmax%' AND is_branded 
          THEN 'Brand Performance Max'
          WHEN "CAMPAIGN.NAME" ILIKE '%pmax%' AND NOT is_branded 
          THEN 'Non-Brand Performance Max'
          WHEN "CAMPAIGN.NAME" ILIKE '%pmax%'  
          THEN 'Performance Max'
          WHEN ("CAMPAIGN.NAME" ILIKE '%youtube%' OR "CAMPAIGN.NAME" ILIKE '%video%' OR ARRAY_TO_STRING(cl.label_name, ',') ILIKE '%youtube%') 
          THEN 'YouTube'
          WHEN ("CAMPAIGN.NAME" ILIKE '%search%' OR ARRAY_TO_STRING(cl.label_name, ',') ILIKE '%search%') AND is_branded 
          THEN 'Brand Search'
          WHEN ("CAMPAIGN.NAME" ILIKE '%search%' OR ARRAY_TO_STRING(cl.label_name, ',') ILIKE '%search%') AND NOT is_branded 
          THEN 'Non-Brand Search'
          WHEN "CAMPAIGN.NAME" ILIKE '%shopping%'
          THEN 'Shopping'
          WHEN "CAMPAIGN.NAME" ILIKE '%demandgen%'
          THEN 'DemandGen'
      END AS campaign_name 
    , DATEADD(hour, "SEGMENTS.HOUR", TO_TIMESTAMP("SEGMENTS.DATE")) AS report_timestamp 
    , "SEGMENTS.DATE" AS report_date 
    , "SEGMENTS.HOUR" AS report_hour 
    , "SEGMENTS.AD_NETWORK_TYPE" AS ad_network
    , "CAMPAIGN.STATUS" AS campaign_status 
    , TRY_TO_DATE("CAMPAIGN.START_DATE") AS campaign_start_date
    , TRY_TO_DATE("CAMPAIGN.END_DATE") AS campaign_end_date 
    , cl.label_name AS campaign_labels
    , "METRICS.CLICKS" AS clicks 
    , "METRICS.CONVERSIONS" AS conversions 
    , "METRICS.COST_MICROS"/1000000 AS cost 
    , "METRICS.IMPRESSIONS" AS impressions 
    , _airbyte_extracted_at 
FROM {{ source('google_ads', 'campaign') }} c 
LEFT JOIN campaign_labels cl
    ON c."CAMPAIGN.ID"=cl.campaign_id
{% if is_incremental() %}
  WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}} )
{%- endif -%}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _airbyte_extracted_at DESC) = 1