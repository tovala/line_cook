
SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(dish_type, '/', 1) AS dish_type
  , SPLIT_PART(dish_format, '/', 1) AS dish_format
FROM {{ ref('meals') }} 

UNION ALL

SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(dish_type, '/', 2) AS dish_type
  , SPLIT_PART(dish_format, '/', 2) AS dish_format
FROM {{ ref('meals') }} 
WHERE dish_type ilike '%/%'
