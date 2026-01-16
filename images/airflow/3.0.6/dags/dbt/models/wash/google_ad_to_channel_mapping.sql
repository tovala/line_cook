
SELECT 
   id
   , campaign_name -- for debugging
   , campaign_labels -- for debugging
   , CASE WHEN campaign_name IN ('PM_gSearch_OvenSales_Branded_QVC', 'Search_US_Brand_Thrive')
               OR ARRAY_SIZE(ARRAY_INTERSECTION(ARRAY_CONSTRUCT('Brand Search'), campaign_labels)) > 0
               OR ARRAY_SIZE(ARRAY_INTERSECTION(ARRAY_CONSTRUCT('BRD', 'Brand'), campaign_labels)) > 0
          THEN 'Google Brand Search'
          WHEN ARRAY_TO_STRING(campaign_labels, ', ') = 'Discovery'
               OR (campaign_labels IS NULL AND campaign_name = 'Google_Discovery_ACQ_tCPA')
          THEN 'Google Discovery'
          WHEN (ARRAY_TO_STRING(campaign_labels, ', ') = 'Ampush' AND campaign_name = 'Google_Display_RMK_ExcludingPastPurchaser_tCPA')
               OR (ARRAY_TO_STRING(campaign_labels, ', ') = 'Display' AND campaign_name = 'Display_BFCM2022-US_Retargeting')
               OR campaign_name ilike '%display%'
          THEN 'Google Display'
          WHEN ARRAY_SIZE(ARRAY_INTERSECTION(ARRAY_CONSTRUCT('Non Brand Search'), campaign_labels)) > 0
               OR ARRAY_SIZE(ARRAY_INTERSECTION(ARRAY_CONSTRUCT('NB'), campaign_labels)) > 0
               OR campaign_name ILIKE '%non%brand%'
          THEN 'Google Non Brand Search'
          WHEN ARRAY_TO_STRING(campaign_labels, ', ') = 'Shopping'
               OR (campaign_labels IS NULL AND campaign_name IN ('Shopping_Brand_eCPC_NA_All', 'Smart Shopping'))
          THEN 'Google Shopping'
          WHEN ARRAY_TO_STRING(campaign_labels, ', ') ILIKE '%youtube%'
               OR (campaign_labels IS NULL AND (campaign_name ILIKE '%youtube%'))
               OR (ARRAY_TO_STRING(campaign_labels, ', ') = 'Ampush' 
                   AND campaign_name IN ('Google_Youtube_All_RMK_tCPA', 'Google_Youtube_All_ACQ_tCPA'))
          THEN 'Google YouTube'
          WHEN campaign_name ILIKE '%demandgen%' 
          THEN 'Google DemandGen'
     END AS channel_name
FROM {{ table_reference('supermetrics_google_ads') }}
