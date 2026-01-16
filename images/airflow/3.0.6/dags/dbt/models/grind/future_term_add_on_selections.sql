
(SELECT 
   ump.id AS add_on_selection_id 
   , aoo.add_on_id 
   , aoo.add_on_offering_id
   , ump.user_id AS customer_id 
   , aoo.menu_id 
   , aoo.term_id
   , aoo.cycle
   , aoo.facility_network
   , ump.mealselectionid AS meal_selection_id 
   , FALSE AS is_coupled  
   , ump.created AS selection_time 
 FROM {{ table_reference('user_menu_product_listing_selections') }} ump
 LEFT JOIN {{ ref('add_on_offerings') }} aoo 
   ON ump.listing_id = aoo.listing_id)
UNION
-- TODO: Remove once coupled add-ons exist in menu_products
( SELECT 
   ms.meal_selection_id AS add_on_selection_id -- Using meal_selection_id b/c it should also be unique in this table
   , aoo.add_on_id 
   , aoo.add_on_offering_id
   , ms.customer_id 
   , aoo.menu_id 
   , aoo.term_id
   , aoo.cycle
   , aoo.facility_network
   , ms.meal_selection_id 
   , TRUE AS is_coupled  
   , ms.meal_selection_time AS selection_time
 FROM {{ ref('future_term_summary') }} fts
 LEFT JOIN {{ ref('meal_selections') }} ms 
   ON fts.customer_id = ms.customer_id 
   AND ms.term_id = fts.term_id
 -- This is terrible logic that won't work if we have multiple menus per subterm, but here we are
 LEFT JOIN {{ ref('menus') }} men
   ON fts.subterm_id = men.subterm_id
 LEFT JOIN {{ table_reference('menu_meals') }} mm 
   ON mm.menu_id = men.menu_id 
   AND mm.meal_id = ms.meal_sku_id
 INNER JOIN {{ ref('add_on_offerings') }} aoo 
   ON mm.id = aoo.paired_menu_meal_id)
