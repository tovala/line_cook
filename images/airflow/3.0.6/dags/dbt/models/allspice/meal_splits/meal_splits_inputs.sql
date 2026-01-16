{{
  config(
    tags=['meal_splits'],
  )
}}

WITH

combined AS (

-- Historical splits (engagement rates)

SELECT
    meal_sku_id::INT AS meal_sku_id
    , term_id
    , cycle
    , facility_network
    , menu_raw_engagement_rate AS split
FROM {{ ref('menu_meal_raw_engagement_rates') }}

UNION ALL

-- Future splits (NULL here; will be filled with predictions)

SELECT
    meal_sku_id::INT AS meal_sku_id
    , term_id
    , cycle
    , facility_network
    , NULL AS split
FROM {{ ref('menu_meal_offerings') }}
WHERE term_id > (SELECT MAX(term_id) FROM {{ ref('menu_meal_raw_engagement_rates') }})

)

SELECT
    -- IDS
    combined.meal_sku_id
    , combined.term_id
    , combined.cycle
    , combined.facility_network
    -- PREDICTION TARGET
    , combined.split
    -- MEAL DETAILS
    , CEIL(meal_skus.meal_price, 2) AS meal_price
    , offerings.main_display_order
    , meal_skus.meal_category
    , meal_skus.serving_count
    , CEIL(meal_skus.base_price, 2) AS base_price
    , CEIL(meal_skus.surcharge_amount, 2) AS surcharge_amount
    , meals.cuisine
    , meals.cuisine_category
    , meals.dish_format
    , meals.dish_type
    , meals.protein_category
    , meals.protein_format
    , meals.protein_type
    , meals.protein_subtype
    , meals.side_category
    , meals.side_type
    , meals.terms_offered_count
    , meals.versions_count
    -- NUTRITION FACTS
    , meal_skus.protein_g
    , meal_skus.calories
    , meal_skus.carbs_g
    , meal_skus.total_fat_g
    , meal_skus.sugar_g
    , meal_skus.sodium_mg
    , meal_skus.saturated_fat_g
    -- BOOL FLAGS
    , meal_skus.is_addon_box::INT AS is_addon_box
    , meal_skus.is_addon_meal::INT AS is_addon_meal
    , meal_skus.is_premium::INT AS is_premium
    , meal_skus.is_best_seller::INT AS is_best_seller
    , meal_skus.is_new_meal::INT AS is_new_meal
    , meal_skus.is_chef_recommendation::INT AS is_chef_recommendation
    , meal_skus.is_vegetarian::INT AS is_vegetarian
    , meal_skus.is_spicy::INT AS is_spicy
    , meal_skus.is_gluten_friendly::INT AS is_gluten_friendly
    , meal_skus.is_low_calorie::INT AS is_low_calorie
    , meal_skus.is_low_carb::INT AS is_low_carb
    , meal_skus.was_frozen_meal::INT AS was_frozen_meal
    , meal_skus.requires_two_min_prep::INT AS requires_two_min_prep
    , meal_skus.contains_bacon_bits::INT AS contains_bacon_bits
    , meal_skus.contains_dessert_addon::INT AS contains_dessert_addon
    , meal_skus.contains_tofu::INT AS contains_tofu
    , meal_skus.contains_pork::INT AS contains_pork
    , meal_skus.is_breakfast::INT AS is_breakfast
    , meal_skus.requires_black_sheet_tray::INT AS requires_black_sheet_tray
    -- INGREDIENTS
FROM combined
LEFT JOIN {{ ref('menu_meal_offerings') }} AS offerings
    ON combined.meal_sku_id = offerings.meal_sku_id
LEFT JOIN {{ ref('meal_skus') }}
    ON offerings.meal_sku_id = meal_skus.meal_sku_id
LEFT JOIN {{ ref('meals') }}
    ON meal_skus.meal_id = meals.meal_id
WHERE offerings.menu_meal_id IS NOT NULL
    AND offerings.term_id IS NOT NULL
