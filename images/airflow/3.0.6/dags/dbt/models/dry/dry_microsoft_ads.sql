{{
  config(
    alias='microsoft_ads',
    materialized='incremental', 
    unique_key="id",
    tags=['online_spend']
  ) 
}}

SELECT 
    _airbyte_raw_id AS id
    , timeperiod AS report_date 
    , network 
    , ACCOUNTNAME AS account_name 
    , DEVICETYPE AS device_type 
    , TOPVSOTHER AS top_vs_other 
    , BIDMATCHTYPE AS bid_match_type
    , ADDISTRIBUTION AS ad_distribution 
    , clicks 
    , spend 
    , conversions 
    , impressions 
    , _airbyte_extracted_at
FROM {{ source('microsoft_ads', 'account_performance_report_daily') }} 
{% if is_incremental() %}
  WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}} )
{%- endif -%}
