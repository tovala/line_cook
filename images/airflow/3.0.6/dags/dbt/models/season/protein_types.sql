
SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(protein_category, '/', 1) AS protein_category
  , SPLIT_PART(protein_type, '/', 1) AS protein_type
  , SPLIT_PART(protein_subtype, '/', 1) AS protein_subtype
  , SPLIT_PART(protein_format, '/', 1) AS protein_format
FROM {{ ref('meals') }} 

UNION ALL

SELECT 
  meal_id
  , internal_meal_name
  , SPLIT_PART(protein_category, '/', 2) AS protein_category
  , SPLIT_PART(protein_type, '/', 2) AS protein_type
  , SPLIT_PART(protein_subtype, '/', 2) AS protein_subtype
  , SPLIT_PART(protein_format, '/', 2) AS protein_format
FROM {{ ref('meals') }} 
WHERE protein_category ilike '%/%'
