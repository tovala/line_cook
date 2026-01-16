
SELECT
  mmo.menu_meal_id
  , mmo.meal_sku_id
  , mmo.term_id
  , mmo.facility_network
  , mmo.cycle
  , mmo.internal_meal_name
--This section calculates running counts for both the sub_term and the terms levels. Again, frozen meals are excluded.
  , ROW_NUMBER() OVER (PARTITION BY mmo.internal_meal_id,mmo.facility_network,mmo.cycle ORDER BY mmo.term_id ASC) AS menu_running_count
  , DENSE_RANK() OVER (PARTITION BY mmo.internal_meal_id ORDER BY mmo.term_id ASC) AS term_running_count
-- This part identifies previous offerings of each meal by utilizing the LAG function. It captures details about the last term in which the meal was offered, 
  , LAG(mmo.term_id) OVER (PARTITION BY mmo.internal_meal_id,mmo.facility_network,mmo.cycle ORDER BY term_id) AS prior_offered_term_id
  , LAG(mmo.menu_meal_id) OVER (PARTITION BY mmo.internal_meal_id,mmo.facility_network,mmo.cycle ORDER BY term_id) AS prior_offered_menu_meal_id
  , LAG(mmo.meal_sku_id) OVER (PARTITION BY mmo.internal_meal_id,mmo.facility_network,mmo.cycle ORDER BY term_id) AS prior_offered_meal_sku_id
  , mmo.term_id - prior_offered_term_id AS terms_since_last_offered
FROM {{ ref('menu_meal_offerings') }} mmo
LEFT JOIN {{ ref('production_meal_tags') }} pmt
  ON mmo.menu_meal_id = pmt.menu_meal_id
WHERE NOT pmt.was_frozen_meal
