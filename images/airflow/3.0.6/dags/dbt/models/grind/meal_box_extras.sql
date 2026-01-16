
SELECT 
  {{ hash_natural_key('me.meal_sku_id ', 'mt.id') }} AS meal_box_extra_map_id
  , me.meal_sku_id 
  , mt.id AS meal_box_extra_id 
  , mt.description AS meal_box_extra_name
FROM {{ table_reference('mealtagmap') }} mtm 
INNER JOIN {{ ref('meal_skus') }} me 
  ON mtm.mealid = me.meal_sku_id 
INNER JOIN {{ table_reference('mealtags') }} mt
  ON mtm.tagid = mt.id
WHERE mt.title LIKE 'BOXEXTRA%'
