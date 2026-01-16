-- Combining all automated online Marketing channel sources into one table 
-- Automatically ingested sources include CTV, Facebook, Google, Youtube, Microsoft, Impact 
-- Due to issues with Tiktok streams, Tiktok spend is currently manually input 

WITH consolidated_sources AS (
    -- GOOGLE ADS 
    SELECT 
        report_date
        , CONCAT('Google Ads - ', campaign_name) AS channel_name
        , 'Google' AS partner  
        , SUM(clicks) AS total_clicks
        , SUM(cost) AS total_spend
        , SUM(conversions) AS total_in_platform_conversions 
        , SUM(impressions) AS total_impressions 
        , 'Google Ads API' AS data_source 
        , MAX(_airbyte_extracted_at) AS upload_time 
        FROM {{ table_reference('google_ads') }}
        GROUP BY ALL 
    UNION 
    -- MICROSOFT ADS 
    SELECT 
        report_date
        , 'Microsoft Ads' AS channel_name 
        , 'Microsoft' AS partner
        , SUM(clicks) AS total_clicks
        , SUM(spend) AS total_spend
        , SUM(conversions) AS total_in_platform_conversions 
        , SUM(impressions) AS total_impressions 
        , 'Bing Ads API' AS data_source 
        , MAX(_airbyte_extracted_at) AS upload_time 
    FROM {{ table_reference('microsoft_ads') }}
    GROUP BY ALL 
    UNION 
    -- FACEBOOK 
    SELECT 
        report_date
        , 'Meta Ads' AS channel_name 
        , 'Meta' AS partner 
        , SUM(clicks) AS total_clicks
        , SUM(spend) AS total_spend
        , NULL AS total_in_platform_conversions
        , SUM(impressions) AS total_impressions 
        , 'Facebook Ads API' AS data_source 
        , MAX(_airbyte_extracted_at) AS upload_time 
    FROM {{ table_reference('facebook_ads') }}
    GROUP BY ALL 
    UNION 
    -- IMPACT 
    SELECT 
        report_date
        , 'Affiliate - Partner Payments' AS channel_name 
        , 'Impact Radius' AS partner 
        , clicks AS total_clicks
        , spend AS total_spend
        , conversions AS total_in_platform_conversions
        , impressions AS total_impressions 
        , 'Impact API' AS data_source 
        , MAX(_airbyte_extracted_at) AS upload_time 
    FROM {{ table_reference('impact_ads') }}
    GROUP BY ALL 
    UNION 
    -- MODUS TV 
    SELECT 
        report_date
        , 'CTV - Modus' AS channel_name 
        , 'Modus Direct' AS partner 
        , NULL AS total_clicks
        , SUM(spend) AS total_spend
        , NULL AS total_in_platform_conversions
        , SUM(impressions) AS total_impressions 
        , 'Modus TV S3' AS data_source 
        , MAX(_airbyte_extracted_at) AS upload_time 
    FROM {{ table_reference('modus_tv_spots') }}
    WHERE report_date < DATEADD('day', -2, current_date())
    GROUP BY ALL 
    )

SELECT 
    {{ hash_natural_key('report_date', 'channel_name', 'partner') }} AS partner_report_id
    , report_date
    , channel_name
    , partner 
    , 'Daily' AS update_cadence
    , 'ETL' AS update_cadence_type 
    , 'Straightline' AS allocation_method
    , total_clicks
    , total_spend
    , total_in_platform_conversions 
    , total_impressions
    , data_source
    , upload_time AS last_updated_at
    , 'ETL' AS last_updated_by
FROM consolidated_sources 