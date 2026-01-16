
-- Flatten the menu to the component level
WITH menu_flattened AS (
  SELECT
    m.value as component
    , m.index AS menu_order_index
    , subterm_id
    , variant_id
  FROM {{ ref('menu_variants_base') }},
  LATERAL FLATTEN(input => RAW_DATA:components) m
),

-- Flatten the textImageStack components again, and get meal_ids
stack_flattened AS (
  SELECT
    menu_order_index
    , c.index AS stack_order_index
    , CASE WHEN c.value:type::STRING = 'meal' THEN ARRAY_CONSTRUCT(c.value:properties:mealID)
          WHEN c.value:type::STRING  = 'mealWithExtra' THEN c.value:properties:mealIDs
          WHEN c.value:type::STRING = 'animatedMealCarousel' THEN TRANSFORM(c.value:properties:mealOptions, item -> item:mealID) 
      END AS meal_ids
    , subterm_id
    , variant_id
  FROM menu_flattened,
  LATERAL FLATTEN(input => component:properties:children) c
  WHERE component:type::STRING = 'textImageStack'
),

-- non-stack components don't need to be flattened again, so we can just get the meal_ids
normal_components AS (
  SELECT 
    menu_order_index
    , 0 AS stack_order_index
    , CASE WHEN component:type::STRING = 'meal' THEN ARRAY_CONSTRUCT(component:properties:mealID)
          WHEN component:type::STRING = 'mealWithExtra' THEN component:properties:mealIDs
          WHEN component:type::STRING = 'animatedMealCarousel' THEN TRANSFORM(component:properties:mealOptions, item -> item:mealID) 
      END AS meal_ids
    , subterm_id
    , variant_id
  FROM menu_flattened
  WHERE component:type::STRING != 'textImageStack'
),

-- Combine the stack and non-stack components
combined_components AS (
  SELECT * FROM stack_flattened
  UNION ALL
  SELECT * FROM normal_components
),

-- Flatten the meal_ids to get the order & individual meal_ids for extras and animatedMealCarousels
ordered_meals AS (
  SELECT
    menu_order_index AS collapsed_display_order
    , m.value AS meal_id
    , ROW_NUMBER() OVER (PARTITION BY subterm_id, variant_id ORDER BY menu_order_index, stack_order_index, m.index) AS menu_display_order
    , subterm_id
    , variant_id
  FROM combined_components,
  LATERAL FLATTEN(input => meal_ids) m
)

-- join in subterm and production_code, get variant_id, and the final order from all the indexes
SELECT
  st.term_id
  , st.facility_network
  , st.cycle
  , om.subterm_id
  , variant_id
  , om.meal_id AS meal_sku_id
  , mm.production_code
  , om.menu_display_order
  , om.collapsed_display_order + 1 as collapsed_display_order
  , {{ hash_menu_variant_id('om.subterm_id', 'variant_id')}} AS menu_variant_id
FROM ordered_meals om
LEFT JOIN {{ ref('menus') }} m
  ON m.subterm_id = om.subterm_id
LEFT JOIN {{ table_reference('menu_meals') }} mm 
  ON om.meal_id = mm.meal_id
  AND mm.menu_id = m.menu_id
LEFT JOIN {{ ref('subterms') }} st
  ON st.subterm_id = om.subterm_id
