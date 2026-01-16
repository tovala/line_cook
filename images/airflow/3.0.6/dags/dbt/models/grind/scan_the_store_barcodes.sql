
WITH most_recent_barcode AS (
  SELECT 
    id AS sts_product_id
    , ROW_NUMBER() OVER (PARTITION BY UPPER(barcode) ORDER BY created DESC) AS row_num 
  FROM {{ table_reference('sts_products') }} 
  WHERE barcode IS NOT NULL
)
SELECT 
  mrb.sts_product_id 
  , UPPER(sts.barcode) AS barcode
  , sts.created AS routine_created_time
  , cohort AS data_source
  , company AS company_name
  , brand AS brand_name 
  , name AS product_name
  , primary_category AS food_category
  , customer_visible 
  , food_packaging_material
  , has_oven_instructions
FROM most_recent_barcode mrb 
INNER JOIN {{ table_reference('sts_products') }} sts
  ON mrb.sts_product_id = sts.id
WHERE mrb.row_num = 1
