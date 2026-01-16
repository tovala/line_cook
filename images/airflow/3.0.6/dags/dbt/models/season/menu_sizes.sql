-- Note this calcualtion is a patch to cusitmozied it meals, more info here 'https://docs.google.com/presentation/d/1mCepgFbAZvD6K8LV_Gssn3VeF0umxmgi8SpRr4xxfJc/edit#slide=id.gccb0294dfc_0_151'

SELECT
  mmo.term_id
  , mmo.cycle
  , mmo.facility_network
  , mmo.menu_id
 -- Adjust menu size for customized it meals,
 -- With base assumpotions: count a dish with multiple protein options as more than one but less than the total number of options.
 -- (1)Dessert and Meal Extras are treated as 1 meals
 -- (2)Dish with two protein options could be 1.5 times a single-option dish
 -- (3) dish with three options could be 1.75 times a single-option dish.
 -- After test and re-iterate the numbers combine with engagment rate buff, the results are following
  , SUM(CASE WHEN customize_it_category IS NULL THEN 1
             WHEN customize_it_category IN ('dessert','meal_extra') THEN 1.03
             WHEN customize_it_category IN ('protein','side') AND customization_option_count = 2 THEN 1.4
             WHEN customize_it_category = 'protein' AND customization_option_count = 3 THEN 1.6
        END) AS menu_size
FROM {{ ref('menu_meal_offerings') }} mmo
LEFT JOIN  {{ ref('customized_it_meals') }} cim
  ON mmo.menu_meal_id = cim.menu_meal_id
LEFT JOIN {{ ref('production_meal_tags') }} pmt
  ON mmo.menu_meal_id = pmt.menu_meal_id
-- meal extras do not increase menu size
WHERE mmo.production_cd > 0
-- frozen meals do not increase menu size
  AND NOT pmt.was_frozen_meal
-- customized secondary meals do not increase menu size
  AND (cim.customized_position = 1 or cim.customized_position IS NULL)
GROUP BY 1,2,3,4