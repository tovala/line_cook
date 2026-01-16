{{
  config(
    tags=["retool"]
  ) 
}}

-- Used by: Demand Planning/Meal Selections 

(SELECT  
   {{ facility_network('fts.facility_network') }} AS facility_network
   , ms.production_cd
   , ms.meal_sku_id
   , ms.meal_title
   , COALESCE(ms.meal_subtitle, '-') AS meal_subtitle
   , ms.term_id
   , sku.meal_price
   , sku.is_surcharged
   , sku.is_breakfast
   , sku.is_dual_serving
   , sku.is_addon_box
   , sku.is_addon_meal
   , sku.contains_dessert_addon
   , FALSE AS is_decoupled_add_on
   , COUNT(DISTINCT CASE WHEN fts.actual_cycle = 1 THEN ms.meal_selection_id END) AS cycle_1_selected
   , COUNT(DISTINCT CASE WHEN fts.actual_cycle = 2 THEN ms.meal_selection_id END) AS cycle_2_selected
   , COUNT(DISTINCT ms.meal_selection_id) AS total_selected
 FROM {{ ref('future_term_summary') }} fts
 INNER JOIN {{ ref('terms') }} te 
   ON fts.term_id = te.term_id
 LEFT JOIN {{ ref('meal_selections') }} ms 
   ON fts.customer_id = ms.customer_id 
   AND ms.term_id = fts.term_id
 LEFT JOIN {{ ref('meal_skus') }} sku
   ON ms.meal_sku_id = sku.meal_sku_id
 WHERE fts.latest_user_status = 'active'
   AND NOT fts.is_skipped
   AND NOT fts.is_internal_account
   AND fts.total_meals_count > 0
   AND te.has_no_orders
 GROUP BY ALL)
UNION 
(SELECT 
   {{ facility_network('fts.facility_network') }} AS facility_network
   , aoo.production_cd
   , NULL AS meal_sku_id
   , ao.add_on_title AS meal_title
   , NULL AS meal_subtitle
   , fts.term_id
   , aoo.price AS meal_price
   , FALSE AS is_surcharged
   , FALSE AS is_breakfast
   , FALSE AS is_dual_serving
   , FALSE AS is_addon_box
   , FALSE AS is_addon_meal
   , FALSE AS contains_dessert_addon
   , TRUE AS is_decoupled_add_on
   , COUNT(DISTINCT CASE WHEN fts.cycle = 1 THEN fts.add_on_selection_id END) AS cycle_1_selected
   , COUNT(DISTINCT CASE WHEN fts.cycle = 2 THEN fts.add_on_selection_id END) AS cycle_2_selected
   , COUNT(DISTINCT fts.add_on_selection_id) AS total_selected
 FROM {{ ref('future_term_add_on_selections') }} fts
 LEFT JOIN {{ ref('add_on_offerings') }} aoo 
   ON fts.add_on_offering_id = aoo.add_on_offering_id
 LEFT JOIN {{ ref('add_ons') }} ao 
   ON aoo.add_on_id = ao.add_on_id
 INNER JOIN {{ ref('terms') }} te
   ON fts.term_id = te.term_id 
 WHERE NOT fts.is_coupled
   AND te.has_no_orders
 GROUP BY ALL)
