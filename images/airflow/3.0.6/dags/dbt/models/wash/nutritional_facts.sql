
SELECT 
  meal_sku_id 
  , meal_info_name AS nutrient
  , meal_info_value AS amount
FROM {{ ref('meal_nutritional_info') }}
WHERE meal_info_type IN ('golden', 'sub')
