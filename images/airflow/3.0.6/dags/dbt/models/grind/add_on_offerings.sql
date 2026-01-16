
(SELECT 
   mpa.id AS add_on_offering_id 
   , mpl.listing_id
   , mpl.product_id AS add_on_id
   , mpl.expiration_date
   , {{ cents_to_usd('mpl.price_cents') }} AS price 
   , mpl.sold_out_count 
   , mpa.menu_id
   , st.term_id 
   , st.cycle 
   , st.facility_network
   , mpa.meal_code AS production_cd 
   , NULL AS paired_menu_meal_id 
   , lay.display_position
   , sec.display_position AS menu_section_position
 FROM {{ table_reference('menu_product_listings') }} mpl
 INNER JOIN {{ table_reference('menu_product_listing_attachments') }} mpa
   ON mpl.listing_id = mpa.listing_id
 LEFT JOIN {{ ref('menus') }} men 
   ON mpa.menu_id = men.menu_id
 LEFT JOIN {{ ref('subterms')}} st
   ON men.subterm_id = st.subterm_id
 LEFT JOIN {{ table_reference('menu_product_listing_sections_layout') }} lay 
   ON mpl.listing_id = lay.listing_id
 LEFT JOIN {{ table_reference('menu_product_listing_sections') }} sec 
   ON lay.section_id = sec.id
 WHERE mpl.deleted IS NULL)
UNION
-- TODO: Remove once coupled add-ons exist in menu_products
(SELECT 
   -- Each coupled add-on should exist on a given menu alongside a given meal exactly once
   {{ hash_natural_key('ao.add_on_id', 'mm.menu_id', 'mm.meal_id') }} AS add_on_offering_id
   , NULL AS listing_id
   , ao.add_on_id
   , mm.expiration_date
   , cao.meal_price AS price
   , meal.sold_out_count 
   , mm.menu_id 
   , st.term_id 
   , st.cycle 
   , st.facility_network
   -- Using 0000 is the convention from menu_meal_offerings
   , '0000' AS production_cd 
   , mm.id AS paired_menu_meal_id
   , NULL AS display_position
   , NULL AS menu_section_position
 FROM {{ ref('coupled_add_ons') }} cao
 LEFT JOIN {{ table_reference('menu_meals') }} mm 
   ON cao.offered_with_meal_id = mm.meal_id
 LEFT JOIN {{ table_reference('meals') }} meal 
   ON mm.meal_id = meal.id 
 LEFT JOIN {{ ref('add_ons') }} ao 
   ON cao.add_on_title = ao.add_on_title
   AND ao.is_coupled
 LEFT JOIN {{ ref('menus') }} men 
   ON mm.menu_id = men.menu_id
 LEFT JOIN {{ ref('subterms')}} st
   ON men.subterm_id = st.subterm_id)
