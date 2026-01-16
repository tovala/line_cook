
(SELECT
-- TODO LIST:
-- 1. add is_experimental_offering boolean: TRUE = this was added to the menu for the sole purpose of testing CX (may be used to exclude specific rows of data for "general" reportingon eng rate etc)
-- 2. add novelty string: "new to customers" vs "new to us (puzzle)" vs "existing" vs "new version"
   mm.id AS menu_meal_id
   , TRY_TO_NUMERIC(mm.meal_id)::INTEGER AS meal_sku_id
   , mm.production_code::INT AS production_cd
   , mm.main_display_order
   , ml.title
   , ml.subtitle
   , st.term_id
   , mn.subterm_id
   , mn.id as menu_id
   , mm.expiration_date
   , st.facility_network
   , st.cycle
   , mns.meal_id AS internal_meal_id
   , {{ hash_natural_key('mns.internal_meal_name', 'st.term_id') }} AS internal_meal_term_id
   , mns.internal_meal_name
   , mns.version_id
   , ml.ingredients
   , {{ cents_to_usd('baseprice_cents') }} AS base_price
   , {{ cents_to_usd('surcharge_cents') }} AS surcharge_amount
   , {{ cents_to_usd('baseprice_cents') }} + {{ cents_to_usd('surcharge_cents') }} AS meal_price
 FROM {{ table_reference('menu_meals') }} mm 
 INNER JOIN {{ table_reference('menus') }} mn 
   ON mm.menu_id = mn.id
 INNER JOIN {{ ref('subterms') }} st  
   ON mn.subterm_id = st.subterm_id
 INNER JOIN {{ table_reference('meals') }} ml
   ON ml.id = mm.meal_id
 INNER JOIN {{ ref('terms') }} t -- only look at terms that actually exist
   ON st.term_id = t.term_id
 LEFT JOIN {{ ref('meal_name_standardizations') }} mns
   ON st.term_id = mns.term_id
   AND mm.production_code::string = mns.production_cd::string 
-- note: joining as strings to bypass non-numeric edge-cases prior to filtering
   AND st.cycle = mns.cycle
   AND st.facility_network = mns.facility_network_name
 WHERE ml.termid IS NOT NULL 
-- Exclude non-live menus 
   AND ml.termid >= 18
-- Exclude fake meal SKU
   AND ml.id <> 77 
-- Exclude skus currently under development
   AND ml.title NOT ILIKE 'WT:%'
-- Exclude non-exist subterms
   AND st.is_available
)
UNION
(SELECT
  {{ hash_natural_key('amo.add_on_meal_id', 'mn.subterm_id') }} AS menu_meal_id
   , amo.add_on_meal_id AS meal_sku_id
   , 0000 AS production_cd
   , NULL AS main_display_order
   , amo.add_on_title AS title
   , NULL as subtitle
   , amo.term_id
   , mn.subterm_id
   , mn.id as menu_id
   , NULL AS expiration_date
   , st.facility_network
   , st.cycle
   , NULL AS internal_meal_id
   , {{ hash_natural_key('amo.add_on_title', 'st.term_id') }} AS internal_meal_term_id
   , amo.add_on_title AS internal_meal_name
   , NULL AS version_id
   , NULL AS ingredients
   , NULL AS base_price
   , NULL AS surcharge_amount
   , NULL AS meal_price
 FROM {{ ref('coupled_add_ons') }} amo
 INNER JOIN {{ table_reference('menu_meals') }} mm 
   ON amo.offered_with_meal_id = mm.meal_id
 INNER JOIN {{ table_reference('menus') }} mn 
   ON mm.menu_id = mn.id
 INNER JOIN {{ ref('subterms') }} st
   ON mn.subterm_id = st.subterm_id
)
