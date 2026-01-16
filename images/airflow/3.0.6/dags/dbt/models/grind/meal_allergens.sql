
WITH all_allergens AS (
  SELECT  
    ni.meal_sku_id
    , v.value::STRING AS allergen
  FROM {{ ref('meal_nutritional_info') }} ni,
    LATERAL FLATTEN(input => SPLIT(meal_info_name, '|')) v
  WHERE ni.meal_info_type = 'allergen')
SELECT 
  meal_sku_id
  , BOOLOR_AGG(allergen = 'eggs') AS contains_egg
  , BOOLOR_AGG(allergen = 'fish') AS contains_fish
  , BOOLOR_AGG(allergen IN ('dairy', 'milk')) AS contains_milk
  , BOOLOR_AGG(allergen = 'peanuts') AS contains_peanut
  , BOOLOR_AGG(allergen IN ('shellfish', 'shrimp')) AS contains_shellfish
  , BOOLOR_AGG(allergen = 'soy') AS contains_soy
  , BOOLOR_AGG(allergen = 'tree nuts') AS contains_tree_nuts
  , BOOLOR_AGG(allergen = 'wheat') AS contains_wheat
FROM all_allergens
GROUP BY 1
