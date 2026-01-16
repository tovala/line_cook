
SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(side_type, '/', 1) AS side_type
  , SPLIT_PART(side_category, '/', 1) AS side_category
FROM {{ ref('meals') }} 

UNION ALL

SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(side_type, '/', 2) AS side_type
  , SPLIT_PART(side_category, '/', 2) AS side_category
FROM {{ ref('meals') }} 
WHERE side_type ilike '%/%'