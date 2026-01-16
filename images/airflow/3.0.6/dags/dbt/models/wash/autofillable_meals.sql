
SELECT 
  term_id 
  , meal_sku_id 
  , contains_pork
  , is_breakfast
  , is_surcharged
FROM {{ ref('meal_skus') }}
-- 1. Exclude add-ons (i.e. biscuits)
WHERE production_cd <> '0000'
  -- 2. Exclude dual-serving meals
  AND NOT COALESCE(is_dual_serving, FALSE)
  -- 3. Exclude surcharged meals > $6
  AND NOT COALESCE(surcharge_amount, 100) > 6.0
