SELECT
  id AS add_on_id
  , title AS add_on_title 
FROM {{ table_reference('meals') }}
WHERE termid IS NULL 
  AND nf_meal_code IS NULL
  AND meal_prep_steps IS NOT NULL
