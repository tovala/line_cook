
(SELECT  
   sel.id AS add_on_selection_id
   , aoo.add_on_offering_id
   , mo.add_on_order_id
   , mo.customer_id
   , sel.menu_id
   , mo.term_id
   , mo.cycle
   , mo.facility_network
   , mo.meal_order_id
   , mo.is_fulfilled
   , sel.refunded AS is_refunded
   , sel.mealselectionid AS meal_selection_id
   , FALSE AS is_coupled
   , sel.created AS selection_time
 FROM {{ table_reference('menu_product_orders') }} mpo
 INNER JOIN {{ ref('meal_orders') }} mo 
   ON mpo.id = mo.add_on_order_id
 INNER JOIN {{ table_reference('menu_product_order_listing_selections') }} sel 
   ON mpo.id = sel.menu_product_order_id
 LEFT JOIN {{ ref('add_on_offerings') }} aoo 
   ON aoo.listing_id = sel.listing_id)
UNION
(SELECT 
   ms.meal_selection_id AS add_on_selection_id -- using as a proxy to check for dupes
   , aoo.add_on_offering_id
   , NULL AS add_on_order_id
   , ms.customer_id
   , aoo.menu_id
   , ms.term_id
   , mo.cycle
   , mo.facility_network
   , ms.meal_order_id
   , ms.is_fulfilled
   -- We can't know if an individual coupled add-on was refunded
   , mo.status = 'refunded' AS is_refunded
   , ms.meal_selection_id
   , TRUE AS is_coupled
   , ms.meal_selection_time AS selection_time
 FROM {{ ref('meal_selections') }} ms
 INNER JOIN {{ ref('add_on_offerings') }} aoo 
   ON ms.menu_meal_id = aoo.paired_menu_meal_id
 LEFT JOIN {{ ref('meal_orders') }} mo
   ON mo.meal_order_id = ms.meal_order_id)
