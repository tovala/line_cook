{{ config(alias='culinary_meal_tags') }}

SELECT DISTINCT
    {{ clean_string('internal_meal_name') }} AS internal_meal_name
    , {{ clean_string('protein_category') }} AS protein_category
    , {{ clean_string('protein_type') }} AS protein_type
    , {{ clean_string('protein_subtype') }} AS protein_subtype 
    , {{ clean_string('protein_format') }} AS protein_format
    , {{ clean_string('cuisine_category') }} AS cuisine_category
    , {{ clean_string('cuisine') }} AS cuisine
    , {{ clean_string('dish_type') }} AS dish_type
    , {{ clean_string('dish_format') }} AS dish_format
    , {{ clean_string('side_category') }} AS side_category
    , {{ clean_string('side_type') }} AS side_type
    , {{ clean_string('health_score') }} AS health_score
    , {{ clean_string('adventurous_score') }} AS adventurous_score
    , {{ clean_string('complex_score') }} AS complex_score
FROM {{ source('meal_tags', 'meal_sku_tags') }}
  -- This indicates that the dish information hasn't been filled out yet
WHERE NOT internal_meal_name IS NULL
  AND NOT (protein_category = '-' AND protein_type = '-' AND protein_subtype = '-' AND protein_format = '-' AND cuisine_category = '-' AND cuisine = '-' AND dish_type = '-' AND dish_format = '-' AND side_category = '-' AND side_type = '-')