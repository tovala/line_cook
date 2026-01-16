{{
  config(
    alias='supermetrics_tiktok_ads',
    materialized='incremental', 
    unique_key='id',
    tags=['supermetrics']
  ) 
}}

SELECT 
  -- There is no primary key in supermetrics, this is a hash of all of the dimensional IDs and the date
  {{ hash_natural_key('ad_group_id::INT', 'ad_id::INT', 'campaign_id::INT', 'date') }} AS id
  , ad_group_id::INT AS ad_group_id
  , ad_group_name
  , ad_id::INT AS ad_id
  , ad_name
  , campaign_id::INT AS campaign_id
  , campaign_name
  , campaign_objective_type
  , date AS report_date
  , clicks
  , complete_payment
  , cost
  , impressions
FROM {{ source('supermetrics', 'tik_tiktok_ads') }}
{{ load_incrementally_supermetrics(30) }}
