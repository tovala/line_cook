-- Note this calcualtion is a patch to cusitmozied it meals, more info here 'https://docs.google.com/presentation/d/1mCepgFbAZvD6K8LV_Gssn3VeF0umxmgi8SpRr4xxfJc/edit#slide=id.gccb0294dfc_0_151'

SELECT
  mmr.menu_meal_id
  -- Normalized raw engagement rate to engagement index by menu size. 
  -- Example: If there are 20 meals on the menu and the avg engagement rate is 5%, then the engagement index for a meal with 5% raw engagement rate is 1.
  -- For protein choice, using historical data from all the meals that were offered as 2 options protein choice, and as an individual meal on a different menu. Compare its engagement index.

  , CASE WHEN (cim.customize_it_category IN ('protein','side') AND cim.customization_option_count = 2)
  -- Add 50% buff to 2 options protein choice meal
         THEN mmr.menu_raw_engagement_rate * 1.5 / (1/ms.menu_size)

         WHEN (cim.customize_it_category = 'protein' AND cim.customization_option_count = 3) 
  -- Add 125% to 3 options protein choice meal
         THEN mmr.menu_raw_engagement_rate * 2.25 / (1/ms.menu_size)

         WHEN (cim.customize_it_category IN ('dessert','meal_extra') AND cim.customized_position = 1) 
  -- For add-ons from 342 tests, 1/3 of the secondary meal engagement will select the primary if only offer the primary option.
         THEN (mmr.menu_raw_engagement_rate + ((cim.group_engagement_rate - mmr.menu_raw_engagement_rate)/3)) / (1/ms.menu_size)

         ELSE mmr.menu_raw_engagement_rate/ (1/ms.menu_size) 
    END AS menu_meal_engagement_index
FROM {{ ref('menu_meal_raw_engagement_rates') }} mmr
LEFT JOIN {{ ref('customized_it_meals') }} cim
  ON mmr.menu_meal_id = cim.menu_meal_id
LEFT JOIN {{ ref('menu_sizes') }} ms
  ON mmr.cycle = ms.cycle
  AND mmr.facility_network = ms.facility_network
  AND mmr.term_id = ms.term_id
