{{
  config(
    alias='impact_ads', 
    materialized='incremental', 
    unique_key="report_date",
    tags=['online_spend']
  ) 
}}

-- Due to constraints on the Impact API, we had to do a one-time download the Performance by Day reports by year
-- We combine these historic datasets into one table and then use the API to pull all future data 

SELECT 
    _airbyte_raw_id AS id 
    , TRY_TO_DATE(date_sort) AS report_date 
    , TRY_TO_NUMBER(clicks)::INTEGER AS clicks 
    , impressions::INT AS impressions 
    , actions::INT AS conversions  
    , totalcost::FLOAT AS spend
    , media_count::INT AS media_count 
    , _airbyte_extracted_at
FROM {{ source('impact', 'performance_by_day') }} 
{% if is_incremental() %}
  WHERE _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}} )
{%- endif -%}
QUALIFY ROW_NUMBER() OVER (PARTITION BY report_date ORDER BY _airbyte_extracted_at DESC) = 1
