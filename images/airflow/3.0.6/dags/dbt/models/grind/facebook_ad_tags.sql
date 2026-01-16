SELECT
  ad_id
  -- lowercase all values since files come in various case types  
  , ad_name
  , LOWER(ad_format) AS ad_format
  , LOWER(campaign_type) AS campaign_type
  , LOWER(campaign_period) AS campaign_period
  , LOWER(sale) AS sale
  , LOWER(price_forward) AS price_forward
  , LOWER(audience) AS audience
  , msa IS NOT NULL AS was_msa
  , sale_price
  , LOWER(main_image) AS main_image
FROM {{ source('sigma_input_tables', 'facebook_ad_tags_output_table') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY last_updated_at DESC) = 1