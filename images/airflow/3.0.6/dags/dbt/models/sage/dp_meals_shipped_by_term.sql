
WITH production_cd_template AS (
  SELECT 
    --Create row for increments of 100 up to 10,000 (a production code for first 100 meals)
    ROW_NUMBER() OVER (ORDER BY seq4())*100 AS production_cd
  FROM TABLE(generator(rowcount => 100))
)
  --Create a table for each is_autoselection option
, boolean_table AS (
  SELECT TO_BOOLEAN(MOD(ROW_NUMBER() OVER (ORDER BY seq4()), 2)) AS is_autoselection
  FROM TABLE(generator(rowcount => 2))
)
, term_meals AS (
  --Match each production code to the correct meal SKU at the subterm and menu level
  SELECT 
    st.subterm_id 
    , st.term_id
    , mm.meal_id AS meal_sku_id
    , pct.production_cd
    , bt.is_autoselection
   FROM production_cd_template pct
   CROSS JOIN boolean_table bt
   CROSS JOIN {{ ref('subterms') }} st
   LEFT JOIN {{ ref('menus') }} m
     ON st.subterm_id = m.subterm_id
   LEFT JOIN {{ table_reference('menu_meals') }} mm
     ON m.menu_id = mm.menu_id
     --Ignore production code with the STRING value 'adm'
     AND pct.production_cd = CASE WHEN mm.production_code = 'adm' THEN NULL ELSE mm.production_code END
)
(SELECT 
   tm.term_id
   , tm.is_autoselection
   , st.facility_network
   , tm.meal_sku_id
   , tm.production_cd
   , ms.meal_title
   , ms.meal_subtitle
   , COUNT(DISTINCT CASE WHEN mo.is_fulfilled AND st.cycle = 1 THEN ms.meal_selection_id END) AS meals_complete_cycle_1
   , COUNT(DISTINCT CASE WHEN mo.is_fulfilled AND st.cycle = 2 THEN ms.meal_selection_id END) AS meals_complete_cycle_2
   , COUNT(DISTINCT CASE WHEN mo.status IN ('payment_error','payment_delinquent', 'payment_authorized_error') AND st.cycle = 1 THEN ms.meal_selection_id END) AS meals_declined_cycle_1
   , COUNT(DISTINCT CASE WHEN mo.status IN ('payment_error','payment_delinquent', 'payment_authorized_error') AND st.cycle = 2 THEN ms.meal_selection_id END) AS meals_declined_cycle_2
 FROM term_meals tm
 LEFT JOIN {{ ref('meal_selections') }} ms
   ON tm.term_id = ms.term_id
   AND tm.meal_sku_id = ms.meal_sku_id
   AND tm.production_cd = ms.production_cd
   AND tm.is_autoselection = ms.is_autoselection
 LEFT JOIN {{ ref('meal_orders') }} mo
   ON ms.customer_id = mo.customer_id
   AND ms.term_id = mo.term_id
 LEFT JOIN {{ ref('subterms') }} st
   ON mo.subterm_id = st.subterm_id
 GROUP BY ALL)
UNION
(SELECT 
   aos.term_id
   , FALSE is_autoselection
   , aos.facility_network
   , NULL AS meal_sku_id
   , off.production_cd
   , ao.add_on_title AS meal_title
   , NULL AS meal_subtitle
   , COUNT(DISTINCT CASE WHEN mo.is_fulfilled AND aos.cycle = 1 THEN aos.add_on_selection_id END) AS meals_complete_cycle_1
   , COUNT(DISTINCT CASE WHEN mo.is_fulfilled AND aos.cycle = 2 THEN aos.add_on_selection_id END) AS meals_complete_cycle_2
   , COUNT(DISTINCT CASE WHEN mo.status IN ('payment_error','payment_delinquent', 'payment_authorized_error') AND aos.cycle = 1 THEN aos.add_on_selection_id END) AS meals_declined_cycle_1
   , COUNT(DISTINCT CASE WHEN mo.status IN ('payment_error','payment_delinquent', 'payment_authorized_error') AND aos.cycle = 2 THEN aos.add_on_selection_id END) AS meals_declined_cycle_2
 FROM {{ ref('add_on_selections') }} aos 
 LEFT JOIN {{ ref('meal_orders') }} mo 
   ON aos.meal_order_id = mo.meal_order_id
 LEFT JOIN {{ ref('add_on_offerings') }} off 
   ON aos.add_on_offering_id = off.add_on_offering_id
 LEFT JOIN {{ ref('add_ons') }} ao 
   ON ao.add_on_id = off.add_on_id
 WHERE NOT aos.is_coupled
 GROUP BY ALL)