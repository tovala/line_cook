{{
  config(
    alias='modus_tv_spots', 
    materialized='incremental', 
    unique_key="id",
    tags=['online_spend']
  ) 
}}

-- From Modus Direct, we pull all relevant TV spot data to track ad performance  
-- Modus dates are set in CST (no timestamp) and a full day's worth of data shows up in the S3 bucket on a day lag 
-- As a result, we can expect aggregates on top of this dataset to differ slightly from Modus's platform 

SELECT 
    -- Chaotic super hash of all dimensions for primary key 
    {{ hash_natural_key('date', 'COALESCE(region, 0::STRING)', 'line_name', 'creative_name') }} AS id 
    , TRY_TO_DATE(date) AS report_date 
    -- Region where the TV spot was aired 
    , region 
    -- Name of TV partner that Modus placed the ad on 
    , line_name AS tv_partner_name 
    -- TV Industry Standard Coding ID 
    -- Modus uses `TO` to indicate Tovala, an integer to indicate # of seconds, `H` as a standard ending 
    , SPLIT_PART(creative_name, '_', 1) AS isci 
    , REGEXP_REPLACE(REPLACE(RTRIM(REPLACE(creative_name, isci, ''), '.mp4'), '_', ' '), ' \\d+\\w', '') AS creative_name  
    , REGEXP_SUBSTR(isci, '\\d+') AS total_seconds 
    , _ab_source_file_url AS source_file_name 
    , _ab_source_file_last_modified AS source_file_last_modified_at
    , impressions::INT AS impressions 
    , spend::FLOAT AS spend
    , _airbyte_extracted_at
FROM {{ source('modus', 'tv_spot') }} 
-- Remove data from before we changed the ingestion strategy to ensure accuracy 
WHERE source_file_last_modified_at>'2025-08-29'
{% if is_incremental() %}
  AND _airbyte_extracted_at >= (SELECT MAX(_airbyte_extracted_at) FROM {{this}} )
{%- endif -%}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_file_last_modified_at DESC) = 1
