
(SELECT 
   product_id AS add_on_id
   , title AS add_on_title
   , misevala_meal_version_id 
   , FALSE AS is_coupled
   , 'TOV' || (ROW_NUMBER() over (order by created))::STRING as short_tag
 FROM {{ table_reference('menu_products') }})
UNION
-- TODO: Remove once coupled add-ons exist in menu_products
(SELECT DISTINCT 
   {{ hash_natural_key('add_on_title') }} AS add_on_id
   , add_on_title
   , NULL AS misevala_meal_version_id 
   , TRUE AS is_coupled
   --placeholder value to never match, avoids matching on nulls
   , 'COUPLED' AS short_tag
 FROM {{ ref('coupled_add_ons') }})
