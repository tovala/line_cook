{{
  config(
    tags=['supermetrics']
  ) 
}}

SELECT
  {{ hash_natural_key('date', 'query','page', 'COALESCE(position,0)') }} AS id
  , date AS report_date
  , CASE 
        WHEN branded_vs_non_branded IN ('branded','non-branded') 
        THEN branded_vs_non_branded 
        ELSE 'unknown' 
    END AS keyword_type
  , page AS landing_page
  , position AS organic_position_in_search
  , position_dim_page AS organic_position_in_search_group
  , query 
  , clicks
  , impressions
FROM {{ source('supermetrics', 'ggl_sc_google_search_console') }}
WHERE date >= DATE('2024-04-12') --data before this date is erroneous
  AND profile = 'https://www.tovala.com/'
  AND query IS NOT NULL --as of 2/20/25, there are only 3 records
