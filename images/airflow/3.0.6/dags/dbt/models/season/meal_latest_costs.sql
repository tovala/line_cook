
SELECT
    mmo.internal_meal_id
  , mmo.term_id
  , mmo.meal_sku_id
  , mmo.internal_meal_name
  , MAX(hmc.historical_ingredient_cost_chicago) AS latest_ingredient_cost_chicago
FROM {{ ref('menu_meal_offerings') }} mmo
INNER JOIN {{ ref('production_meal_tags') }} pmt
 ON mmo.menu_meal_id = pmt.menu_meal_id
 AND NOT pmt.was_frozen_meal
INNER JOIN {{ ref('historical_meal_costs') }} hmc
 ON mmo.meal_sku_id = hmc.meal_sku_id
WHERE mmo.internal_meal_name IS NOT NULL
GROUP BY 1,2,3,4
QUALIFY ROW_NUMBER() OVER (PARTITION BY mmo.internal_meal_id ORDER BY mmo.term_id DESC) = 1
