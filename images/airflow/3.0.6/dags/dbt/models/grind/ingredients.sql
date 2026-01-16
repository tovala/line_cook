
SELECT 
  DISTINCT
  {{ hash_natural_key('LOWER(mpg.name)', 'mpg.id') }} AS ingredient_id --unique key
  , mpg.id AS purchased_good_id
  , LOWER(mpg.name) AS ingredient_name
--Used similar system to grind.meal_allergens
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%egg %', '%eggs %') THEN 1 ELSE 0 END AS contains_egg
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%fish %','%anchovy %', '%anchovies %', '%bonito %', '%cod %', '%pollock %','%salmon %', '%mahi mahi %','%trout %') THEN 1 ELSE 0 END AS contains_fish
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%milk %', '%dairy %') THEN 1 ELSE 0 END AS contains_dairy
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE '%peanut %' THEN 1 ELSE 0 END AS contains_peanut
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%shrimp %','%shellfish %', '%clam%','%lobster%', '%crab%', '%oyster%') AND NOT LOWER(mpg.name) LIKE '%cracker%' THEN 1 ELSE 0 END AS contains_shellfish
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%soy %','%soybean%') THEN 1 ELSE 0 END AS contains_soy
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE ANY ('%coconut %','%pistachio%', '%corn nut%','%walnut%', '%almond%', '%cashew%','%pecan%','%macadamia%','%tree nut%') THEN 1 ELSE 0 END AS contains_tree_nuts
  , CASE WHEN REGEXP_REPLACE(ingredient_name,'[^a-z ]', '') LIKE '%wheat %' THEN 1 ELSE 0 END AS contains_wheat
  , CASE WHEN ingredient_name LIKE '%water%'
         THEN 'water' 
         WHEN ingredient_name IN ('carrot, frozen, diced, 3/8', 'puree, ginger')
         THEN 'frozen_items'
         WHEN ingredient_name LIKE 'celery sa%'
         THEN 'produce'
         WHEN ingredient_name LIKE 'chicken,%'
         THEN 'meat'
         WHEN ingredient_name IN ('cilatnro, dried', 'curry powder, madras', 'parsley, dried', 'smoked sweet paprika')
         THEN 'standard_spices'
         WHEN ingredient_name LIKE 'diced onoin sa%' OR ingredient_name = 'onion, yellow, diced 4/5'
         THEN 'precut_vegetables'
         WHEN ingredient_name IN ('sauce, hot, tobasco, packet', 'tomatoes, diced, in juice', 'wine, franzia. red')
         THEN 'prepared_items'
         WHEN ingredient_name = 'oil, grapseed'
         THEN 'oil_and_vinegar'
         ELSE REPLACE(LOWER(mpg.category), ' ', '_') 
    END AS ingredient_category
FROM {{ table_reference('mise_purchased_goods') }} mpg
JOIN {{ table_reference('mise_part_pgs') }} mppgs
  ON mpg.id = mppgs.purchased_good_id
JOIN {{ table_reference('mise_production_part_boms') }} mppb --by linking to boms, all ingredients that were never used in a term are removed
  ON mppgs.part_id = mppb.part_id