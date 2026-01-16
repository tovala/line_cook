
SELECT
  meal_sku_id
  , MAX(CASE WHEN nutrient = 'calories' 
             THEN amount
        END) AS calories
  , MAX(CASE WHEN nutrient = 'carbs' 
             THEN amount
        END) AS carbs_g
  , MAX(CASE WHEN nutrient = 'protein' 
             THEN amount
        END) AS protein_g
  , MAX(CASE WHEN nutrient = 'saturated fat' 
             THEN amount
        END) AS saturated_fat_g
  , MAX(CASE WHEN nutrient = 'total fat' 
             THEN amount
        END) AS total_fat_g
  , MAX(CASE WHEN nutrient = 'sugar' 
             THEN amount
        END) AS sugar_g
  , MAX(CASE WHEN nutrient = 'sodium' 
             THEN amount
        END) AS sodium_mg
FROM {{ ref('nutritional_facts') }} 
GROUP BY 1
