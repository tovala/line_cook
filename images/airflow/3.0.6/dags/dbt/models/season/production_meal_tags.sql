
SELECT 
  mmo.menu_meal_id    
  , mmo.meal_sku_id
  , MAX(COALESCE(mmo.ingredients ILIKE '%pork%' OR mmo.ingredients ILIKE '%bacon%' OR mmo.ingredients ILIKE '% ham%', FALSE)) AS contains_pork
  , MAX(COALESCE(mmo.ingredients ILIKE '%tofu%', FALSE)) AS contains_tofu
  , MAX(COALESCE(mt.meal_tag = 'add_on_box', FALSE)) AS is_addon_box
  , MAX(COALESCE(mt.meal_tag = 'add_on_meal', FALSE)) AS is_addon_meal
  , MAX(COALESCE(mt.meal_tag = 'contains_dessert_addon', FALSE)) AS contains_dessert_addon
  , MAX(COALESCE(mt.meal_tag = 'hidden_meal', FALSE)) AS was_frozen_meal
  , MAX(COALESCE(mt.meal_tag = 'breakfast', FALSE)) AS is_breakfast
  , MAX(COALESCE(mt.meal_tag = '2_servings_exclude_from_autofill', FALSE)) AS is_dual_serving
  , MAX(COALESCE(mt.meal_tag = 'family_meal_serves_4_together', FALSE)) AS is_family_meal
  , MAX(COALESCE(mt.meal_tag = 'premium', FALSE)) AS is_premium
  , MAX(COALESCE(mmo.surcharge_amount > 0, FALSE)) AS is_surcharge
  , is_surcharge AND NOT is_premium AS is_non_premium_surcharge
FROM {{ ref('menu_meal_offerings') }} mmo
LEFT JOIN {{ ref('meal_tags') }} mt
  ON mmo.meal_sku_id = mt.meal_sku_id
GROUP BY 1,2
