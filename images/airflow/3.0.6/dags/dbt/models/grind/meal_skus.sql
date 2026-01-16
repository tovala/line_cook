
WITH meal_tags AS (
  SELECT 
    mtm.mealid AS meal_sku_id
    , MAX(COALESCE(mtm.tagid = 80, FALSE)) AS is_addon_box
    , MAX(COALESCE(mtm.tagid = 81, FALSE)) AS is_addon_meal
    , MAX(COALESCE(mtm.tagid = 39, FALSE)) AS is_best_seller
    , MAX(COALESCE(mtm.tagid = 58, FALSE)) AS requires_black_sheet_tray
    , MAX(COALESCE(mtm.tagid = 61, FALSE)) AS is_breakfast
    , MAX(COALESCE(mtm.tagid IN (40, 24), FALSE)) AS is_low_calorie
    , MAX(COALESCE(mtm.tagid IN (45, 31), FALSE)) AS is_low_carb
    , MAX(COALESCE(mtm.tagid IN (32, 38), FALSE)) AS is_chef_recommendation
    , MAX(COALESCE(mtm.tagid IN (3, 42), FALSE)) AS is_gluten_friendly
    , MAX(COALESCE(mtm.tagid IN (1, 41), FALSE)) AS is_vegetarian
    , MAX(COALESCE(mtm.tagid = 64, FALSE)) AS is_premium
    , MAX(COALESCE(mtm.tagid = 65, FALSE)) AS is_dual_serving
    , MAX(COALESCE(mtm.tagid = 73, FALSE)) AS is_spicy
    , MAX(COALESCE(mtm.tagid = 53, FALSE)) AS was_frozen_meal
    , MAX(COALESCE(mtm.tagid = 66, FALSE)) AS contains_2_servings
    , MAX(COALESCE(mtm.tagid = 82, FALSE)) AS is_chefs_collab
    , MAX(COALESCE(mtm.tagid = 88, FALSE)) AS contains_dessert_addon
    , MAX(COALESCE(mtm.tagid = 86, FALSE)) AS is_new_meal
    , MAX(COALESCE(mtm.tagid = 89, FALSE)) AS requires_two_min_prep
  FROM {{ table_reference('mealtags') }} mt 
  INNER JOIN {{ table_reference('mealtagmap') }} mtm
    ON mtm.tagid = mt.id
  GROUP BY 1
)
(SELECT DISTINCT
    m.id AS meal_sku_id  
    , m.title 
    , subtitle 
    , m.termid AS term_id 
    , mm.production_code AS production_cd 
    , {{ cents_to_usd('baseprice_cents') }} AS base_price
    , {{ cents_to_usd('surcharge_cents') }} AS surcharge_amount
    , COALESCE(
        ( {{ cents_to_usd('baseprice_cents') }} + {{ cents_to_usd('surcharge_cents') }} ), 
        price) 
      AS meal_price
    , created_by AS chef 
    -- TODO: Figure out if NULL is the appropriate thing to have (should it be True vs Null or True/Null/False ?)
    , first_ship_period_availability AS is_available_in_cycle_1 
    , second_ship_period_availability AS is_available_in_cycle_2 
    , COALESCE(mt.is_premium, FALSE) AS is_premium
    , COALESCE(surcharge_cents::int > 0, FALSE) AND NOT COALESCE(mt.is_premium, FALSE) AS is_non_premium_surcharge
    , COALESCE(mt.is_best_seller, FALSE) AS is_best_seller
    , COALESCE(mt.requires_black_sheet_tray, FALSE) AS requires_black_sheet_tray
    , CASE WHEN mt.contains_2_servings OR mt.is_dual_serving THEN 2 ELSE 1 END AS serving_count
    , COALESCE(mt.is_breakfast, FALSE) AS is_breakfast
    , COALESCE(mt.is_low_calorie, FALSE) AS is_low_calorie
    , COALESCE(mt.is_low_carb, FALSE) AS is_low_carb
    , COALESCE(mt.is_chef_recommendation, FALSE) AS is_chef_recommendation
    , COALESCE(mt.is_chefs_collab, FALSE) AS is_chefs_collab
    , COALESCE(mt.is_gluten_friendly, FALSE) AS is_gluten_friendly
    , COALESCE(mt.is_vegetarian, FALSE) AS is_vegetarian
    , COALESCE(mt.is_spicy, FALSE) AS is_spicy
    , COALESCE(mt.is_addon_box, FALSE) AS is_addon_box
    , COALESCE(mt.is_addon_meal, FALSE) AS is_addon_meal
    , COALESCE(mt.was_frozen_meal, FALSE) AS was_frozen_meal
    , COALESCE(mt.contains_dessert_addon, FALSE) AS contains_dessert_addon
    , COALESCE(mt.is_new_meal, FALSE) AS is_new_meal
    , COALESCE(mt.requires_two_min_prep, FALSE) AS requires_two_min_prep
    -- Breakfast meals can be tagged as dual_serving but they shouldn't have the flag for our purposes
    , CASE WHEN is_breakfast
           THEN FALSE
           ELSE COALESCE(mt.is_dual_serving, FALSE) 
      END AS is_dual_serving
    , CASE WHEN surcharge_amount <> 0 THEN true 
           ELSE false
      END AS is_surcharged  
    , CASE WHEN is_breakfast THEN 'breakfast'
           WHEN is_dual_serving THEN 'dual_serving'
           WHEN is_surcharged THEN 'surcharged'
           ELSE 'base'
      END AS meal_category
    -- Note: Space on '% ham%' is intentional (otherwise things like “champagne vinegar” will get flagged)
    -- TODO: Figure out if we want to use REGEXP or ILIKE
    , m.ingredients AS ingredient_list
    -- 'ims' enables case-insensitive multiline searching for pork, ham, and bacon
    , COALESCE(REGEXP_LIKE(m.ingredients, '.*\\b(pork|ham|bacon)\\b.*', 'ims'), FALSE) AS contains_pork
    -- Note: if additional pork garnishes are added, we need to update this logic
    , COALESCE(REGEXP_LIKE(m.ingredients, '.*\\b(bacon bits)\\b.*', 'ims'), FALSE) AS contains_bacon_bits
    , COALESCE(m.ingredients ILIKE '%tofu%', FALSE) AS contains_tofu
    , hmc.historical_ingredient_cost_chicago AS cogs
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 1 THEN mm.menu_id END) AS chi_1_menu_id
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 2 THEN mm.menu_id END) AS chi_2_menu_id
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 1 THEN mm.menu_id END) AS slc_1_menu_id
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 2 THEN mm.menu_id END)AS slc_2_menu_id
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 1 THEN mm.expiration_date END) AS chi_1_expiration_date
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 2 THEN mm.expiration_date END) AS chi_2_expiration_date
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 1 THEN mm.expiration_date END) AS slc_1_expiration_date
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 2 THEN mm.expiration_date END) AS slc_2_expiration_date
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 1 THEN msl.sleeving_location END) AS chi_1_sleeving_location
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 2 THEN msl.sleeving_location END) AS chi_2_sleeving_location
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 1 THEN msl.sleeving_location END) AS slc_1_sleeving_location
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 2 THEN msl.sleeving_location END) AS slc_2_sleeving_location
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 1 THEN msl.sleeving_configuration END) AS chi_1_sleeving_configuration
    , MAX(CASE WHEN st.facility_network = 'chicago' AND st.cycle = 2 THEN msl.sleeving_configuration END) AS chi_2_sleeving_configuration
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 1 THEN msl.sleeving_configuration END) AS slc_1_sleeving_configuration
    , MAX(CASE WHEN st.facility_network = 'slc' AND st.cycle = 2 THEN msl.sleeving_configuration END) AS slc_2_sleeving_configuration
    , MAX(CASE WHEN nf.nutrient = 'calories' THEN nf.amount ELSE NULL END) AS calories
    , MAX(CASE WHEN nf.nutrient = 'carbs' THEN nf.amount ELSE NULL END) AS carbs_g
    , MAX(CASE WHEN nf.nutrient = 'protein' THEN nf.amount ELSE NULL END) AS protein_g
    , MAX(CASE WHEN nf.nutrient = 'saturated fat' THEN nf.amount ELSE NULL END) AS saturated_fat_g
    , MAX(CASE WHEN nf.nutrient = 'total fat' THEN nf.amount ELSE NULL END) AS total_fat_g
    , MAX(CASE WHEN nf.nutrient = 'sugar' THEN nf.amount ELSE NULL END) AS sugar_g
    , MAX(CASE WHEN nf.nutrient = 'sodium' THEN nf.amount ELSE NULL END) AS sodium_mg 
    , CASE 
        WHEN COALESCE(chi_1_menu_id, chi_2_menu_id) IS NOT NULL AND COALESCE(slc_1_menu_id, slc_2_menu_id) IS NOT NULL THEN 'chicago_slc'
        WHEN COALESCE(chi_1_menu_id, chi_2_menu_id) IS NOT NULL THEN 'chicago'
        WHEN COALESCE(slc_1_menu_id, slc_2_menu_id) IS NOT NULL THEN 'slc'
      END AS facility_network 
    , NULLIF(LISTAGG(DISTINCT mns.meal_id, '|'), '') AS meal_id
    , NULLIF(LISTAGG(DISTINCT mns.internal_meal_name, '|'), '') AS internal_meal_name
FROM {{ table_reference('meals') }} m 
LEFT JOIN {{ ref('nutritional_facts') }} nf 
  ON m.id = nf.meal_sku_id
LEFT JOIN meal_tags mt 
  ON m.id = mt.meal_sku_id
LEFT JOIN {{ table_reference('menu_meals') }} mm
  ON m.id = mm.meal_id
LEFT JOIN {{ table_reference('menus') }} me
  ON mm.menu_id = me.id
LEFT JOIN {{ ref('subterms') }} st
  ON me.subterm_id = st.subterm_id
--Get internal meal name
LEFT JOIN {{ ref('meal_name_standardizations') }} mns
  ON m.termid = mns.term_id
  AND mm.production_code = mns.production_cd
  AND st.cycle = mns.cycle
  AND st.facility_network = mns.facility_network_name
LEFT JOIN {{ ref('historical_meal_costs') }} hmc 
  ON m.id = hmc.meal_sku_id
LEFT JOIN {{ ref('meal_sleeves') }} msl
  ON CONTAINS(st.facility_network, msl.facility_network)
  AND msl.term_id = m.termid
  AND msl.production_cd = mm.production_code
  AND msl.cycle = st.cycle
WHERE m.termid IS NOT NULL 
  -- Exclude non-live menus 
  AND m.termid >= 18
  -- Exclude fake meal SKU
  AND m.id <> 77 
  -- Exclude skus currently under development
  AND m.title NOT ILIKE 'WT:%'
  AND st.is_available
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38)
UNION
(
 SELECT 
  add_on_meal_id AS meal_sku_id 
  , add_on_title AS title 
  , NULL AS subtitle
  , term_id
  , '0000' AS production_cd
  , meal_price AS base_price
  , NULL AS surcharge_amount
  , meal_price 
  , NULL AS chef
  , NULL AS is_available_in_cycle_1
  , NULL AS is_available_in_cycle_2
  , NULL AS is_premium
  , NULL AS is_non_premium_surcharge 
  , NULL AS is_best_seller
  , NULL AS requires_black_sheet_tray
  , NULL AS serving_count 
  , NULL AS is_breakfast
  , NULL AS is_low_calorie
  , NULL AS is_low_carb
  , NULL AS is_chef_recommendation
  , NULL AS is_chefs_collab
  , NULL AS is_gluten_friendly
  , NULL AS is_vegetarian
  , NULL AS is_spicy
  , NULL AS is_addon_box
  , TRUE AS is_addon_meal
  , NULL AS was_frozen_meal
  , NULL AS contains_dessert_addon
  , NULL AS is_new_meal
  , NULL AS requires_two_min_prep
  , NULL AS is_dual_serving
  , NULL AS is_surcharged
  , NULL AS meal_category
  , NULL AS ingredient_list
  , NULL AS contains_pork
  , NULL AS contains_bacon_bits
  , NULL AS contains_tofu
  , NULL AS cogs
  , NULL AS chi_1_menu_id
  , NULL AS chi_2_menu_id
  , NULL AS slc_1_menu_id
  , NULL AS slc_2_menu_id
  , NULL AS chi_1_expiration_date
  , NULL AS chi_2_expiration_date
  , NULL AS slc_1_expiration_date
  , NULL AS slc_2_expiration_date
  , NULL AS chi_1_sleeving_location
  , NULL AS chi_2_sleeving_location
  , NULL AS slc_1_sleeving_location
  , NULL AS slc_2_sleeving_location
  , NULL AS chi_1_sleeving_configuration
  , NULL AS chi_2_sleeving_configuration
  , NULL AS slc_1_sleeving_configuration
  , NULL AS slc_2_sleeving_configuration
  , NULL AS calories
  , NULL AS carbs_g
  , NULL AS protein_g
  , NULL AS saturated_fat_g
  , NULL AS total_fat_g
  , NULL AS sugar_g
  , NULL AS sodium_mg
  , 'chicago_slc' AS facility_network
  , NULL AS meal_id
  , add_on_title AS internal_meal_name
  FROM {{ ref('coupled_add_ons') }}
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY add_on_meal_id 
      ORDER BY term_id DESC
  ) = 1 
)