-- This table is replacing all the 2nd position 'dessert','meal_extra','dual_serving' with corresponding customized it group's the 1st position IDs.
-- Because the 2nd option is essentially the same meal as the 1st option. 

SELECT
  ms.meal_selection_id
  , ms.customer_id
  , ms.term_id
  , COALESCE(cim2.meal_sku_id,ms.meal_sku_id) AS meal_sku_id
  , COALESCE(cim2.menu_meal_id,ms.menu_meal_id) AS menu_meal_id
FROM {{ ref('meal_selections') }} ms 
LEFT JOIN {{ ref('customized_it_meals') }} cim1
  ON ms.menu_meal_id = cim1.menu_meal_id
LEFT JOIN {{ ref('customized_it_meals') }} cim2
-- Join on the same customized it meal group 
  ON cim1.customize_it_menu_meal_group = cim2.customize_it_menu_meal_group
-- Only replacing 'dessert','meal_extra','dual_serving' meals
  AND cim1.customize_it_category IN ('dessert','meal_extra','dual_serving')
-- Filter out some rare cases
  AND (cim1.customize_it_merchandising IS NULL OR cim1.customize_it_merchandising NOT IN ('protein_choice','protein_swap'))
-- This will create the "mismatch" with in group
  AND cim1.menu_meal_id <> cim2.menu_meal_id
-- Replacing 2nd options
  AND cim1.customized_position = 2
WHERE ms.is_fulfilled
AND NOT ms.is_autoselection
AND ms.menu_meal_id IS NOT NULL
