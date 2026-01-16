
WITH surcharge_sum AS (
  SELECT
    mmo.menu_meal_id
    , mmo.meal_sku_id
    , cim.customize_it_category
    , cim.customized_position
    , COALESCE(mmo.surcharge_amount, 0) AS surcharge_amount
    , pmt.is_breakfast
    , pmt.is_dual_serving
    , pmt.is_family_meal
-- Anything has breakfast tag(some of them are dual-serving)
    , CASE WHEN pmt.is_breakfast AND NOT pmt.is_family_meal
           THEN mmo.surcharge_amount
           ELSE 0
      END AS breakfast_surcharge_amount
-- Anything has dual_serving tag(exclude breakfast)
    , CASE WHEN pmt.is_dual_serving AND NOT pmt.is_breakfast
           THEN mmo.surcharge_amount
           ELSE 0
      END AS dual_serving_surcharge_amount
    , CASE WHEN pmt.is_family_meal
           THEN mmo.surcharge_amount
           ELSE 0
      END AS family_meal_surcharge_amount
-- note1: sum of surcharge of customize_it_menu_meal_group is using to seperate premium and meal_extra in next table
    , SUM(mmo.surcharge_amount) OVER (PARTITION BY cim.customize_it_menu_meal_group) AS customize_group_surcharge_amount
  FROM {{ ref('menu_meal_offerings') }} mmo
  LEFT JOIN {{ ref('customized_it_meals') }} cim
    ON mmo.menu_meal_id = cim.menu_meal_id
  LEFT JOIN {{ ref('production_meal_tags') }} pmt
    ON mmo.menu_meal_id = pmt.menu_meal_id)

SELECT DISTINCT
  menu_meal_id
  , meal_sku_id 
  , surcharge_amount
  , breakfast_surcharge_amount
  , dual_serving_surcharge_amount
  , family_meal_surcharge_amount
-- Some dessert and meal_extra are also premium, they have 2 surcharges

-- e.g. Spanish-Style Garlic Tomato Shrimp $2.99 = premium 
--      Spanish-Style Garlic Tomato Shrimp with Sticky Toffee Cake $7.99 = premium + dessert
--      dessert = $7.99 - $2.99 = $5

-- Algorithm: to get that $5 for 2nd position meal_extra surcharge 
--         =  ( desssert + premium ) - premium
--         = (meal_extra_surcharge_amount + premium_surcharge_amount) - premium_surcharge_amount
--         = total surcharge of the meal with meal extra -  total surcharge of the meal WITHOUT meal extra
--         = total surcharge of the meal with meal extra - ( sum of surcharge of the customized meal pair(note1) - total surcharge of the meal with meal extra)
--         Note1: I need $2.99 to be at the same row with $7.99 in order to do the subtraction.
--         By doing the PARTITION BY clause in NOTE1 I can get ($2.99+$7.99) in the the same row with the $7.99 
-- e.g.      7.99 - ((2.99 + 7.99) - 7.99)  
--         = $ 5
  , CASE WHEN surcharge_amount = 0
         THEN 0
         WHEN customize_it_category = 'dessert' AND customized_position = 2
         THEN surcharge_amount - (customize_group_surcharge_amount - surcharge_amount)
         ELSE 0
    END AS dessert_surcharge_amount
  , CASE WHEN surcharge_amount = 0
         THEN 0
         WHEN customize_it_category = 'meal_extra' AND customized_position = 2
         THEN surcharge_amount - (customize_group_surcharge_amount - surcharge_amount)
         ELSE 0
    END AS meal_extra_surcharge_amount
-- Using the amount of surcharge for meal_extra/dessert we got from above,
-- we can calculate the premium portion of the surcharge for premium dessert/meal_extra meals. 

-- premium_surcharge_amount = surcharge_amount - dessert_surcharge_amount

-- e.g. 7.99 - 5 = 2.99

-- Anything that is not breakfast AND dual_serving, all surcharges are premium
  , CASE WHEN surcharge_amount = 0
         THEN 0
         WHEN customize_it_category = 'dessert' AND customized_position = 1
         THEN surcharge_amount
         WHEN customize_it_category = 'dessert' AND customized_position = 2
         THEN surcharge_amount - dessert_surcharge_amount
         WHEN customize_it_category = 'meal_extra' AND customized_position = 1
         THEN surcharge_amount
         WHEN customize_it_category = 'meal_extra' AND customized_position = 2
         THEN surcharge_amount - meal_extra_surcharge_amount
         WHEN NOT is_breakfast AND NOT is_dual_serving AND NOT is_family_meal
         THEN surcharge_amount
         ELSE 0
    END AS premium_surcharge_amount
FROM surcharge_sum 
