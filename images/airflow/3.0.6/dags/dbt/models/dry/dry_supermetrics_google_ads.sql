{{
  config(
    alias='supermetrics_google_ads',
    materialized='incremental', 
    unique_key='id',
    tags=['supermetrics']
  ) 
}}

SELECT 
  -- There is no primary key in supermetrics, this is a hash of all of the dimensional IDs and the date
  {{ hash_natural_key('ad_group_id::INT', 'ad_id::INT', 'campaign_id::INT', 'profile_id::INT', 'date') }} AS id
  , ad_group_id::INT AS ad_group_id
  , {{ array_string_to_variant('ad_group_labels') }} AS ad_group_labels 
  , ad_group_name
  , ad_id::INT AS ad_id
  , {{ array_string_to_variant('ad_labels') }} AS ad_labels
  , campaign_id::INT AS campaign_id 
  , {{ array_string_to_variant('campaign_labels') }} AS campaign_labels
  , campaign_name
  , date AS report_date
  , profile AS profile_name
  , profile_id::INT AS profile_id
  , clicks
  , conversions
  , cost
  , impressions
FROM {{ source('supermetrics', 'googleads_google_ads') }}
{{ load_incrementally_supermetrics(30) }}
