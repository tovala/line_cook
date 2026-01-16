
WITH clean_tags AS (
    SELECT
      part_title
      , part_id
      , parent_production_part_id
      , parse_json(tags):tags AS tags
      , qty_pounds
    FROM {{ table_reference('mise_production_part_boms') }}
)
, unpacked AS (
    SELECT 
      ct.*
      , value:category::string AS cat
      , value:title::string AS val
    FROM clean_tags ct, LATERAL FLATTEN (input => tags)
)
, meal_components AS (
    SELECT 
       LOWER(up.part_title) AS component_name
      , up.part_id AS component_id  
      , ms.meal_sku_id
      , mns.internal_meal_name
      , mns.meal_id
      , mns.version_id
      , mns.cycle
      , mfn.title AS facility_network
      , mns.term_id
      , mns.production_cd
      , up.qty_pounds AS component_weight_lbs
      --portion tags
      , MAX(CASE WHEN up.cat = 'portion_location' THEN up.val END) AS location
      , MAX(CASE WHEN up.cat = 'portion_date' THEN up.val END) AS portion_date
      , MAX(CASE WHEN up.cat = 'container' THEN up.val END) AS container
      , MAX(CASE WHEN up.cat = 'spare_tray' THEN up.val END) AS spare_tray
      , MAX(CASE WHEN up.cat = 'frozen' THEN up.val END) AS frozen
      , MAX(CASE WHEN up.cat = 'MAP' THEN up.val END) AS map
      , MAX(CASE WHEN up.cat = 'film_type' THEN up.val END) AS film_type
      , CASE WHEN container LIKE 'tray 1%' THEN 'Component 1'
             WHEN container LIKE 'tray 2%' OR container = 'veggie bags' THEN 'Component 2'
             WHEN container LIKE ANY ('bag','%bag','%bags','sealed body armor','2 oz cup','2 oz oval cup','1 oz cup','dry sachet','sachet','pre-portioned','N/A','clamshell','') THEN 'Garnish' 
             ELSE 'Mislabeled'
        END AS component_type
    FROM {{ ref('meal_name_standardizations') }} mns
    INNER JOIN {{ table_reference('mise_production_parts') }} mpp
      ON mns.part_id = mpp.part_id
      AND mns.cycle = mpp.cycle
      AND mns.facility_network_id = mpp.facility_network_id
      AND mns.term_id = mpp.term_id
    INNER JOIN unpacked up
      ON mpp.id = up.parent_production_part_id
    LEFT JOIN {{ table_reference('mise_facility_network') }} mfn
      ON mns.facility_network_id = mfn.id
    LEFT JOIN {{ ref('meal_skus') }} ms
      ON mns.production_cd = ms.production_cd
      AND mns.term_id = ms.term_id
      AND CONTAINS(ms.facility_network, mfn.title) --facility_network in meal_skus is concatenated
      AND mns.meal_id = ms.meal_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
SELECT 
  --Components can be duplicated in name with "-2" within the same production_cd and contains two different weights
   {{ hash_natural_key('component_name', "COALESCE('component_id', '')", 'term_id','cycle','facility_network','production_cd','component_type') }} AS meal_component_facility_id --unique key
  , mc.component_id
  , mc.component_name
  , mc.meal_sku_id
  , mc.internal_meal_name
  , mc.meal_id
  , mc.version_id
  , mc.term_id
  , mc.cycle
  , mc.facility_network
  , mc.production_cd
  , mc.component_weight_lbs
  , mc.location
  , mc.portion_date
  , mc.container
  , mc.spare_tray
  , mc.frozen
  , mc.map
  , mc.film_type
  , mc.component_type
  , CASE WHEN mc.component_name LIKE 'cornbread%'
         THEN 'cornbread'
         WHEN mc.component_name LIKE 'fried rice%'
         THEN 'fried rice'
         WHEN mc.component_name LIKE 'lasagna%'
         THEN 'lasagna'
         WHEN (mc.component_name LIKE '%mac mix%' OR mc.component_name LIKE '%macaroni%' OR mc.component_name LIKE '%mac and cheese%')
         THEN 'mac_and_cheese'
         WHEN mc.component_name LIKE 'penne%'
         THEN 'penne'
         WHEN mc.component_name LIKE 'risotto%'
         THEN 'risotto'
         WHEN (mc.component_name LIKE '%stuffed shells%' OR mc.component_name LIKE '%shells')
         THEN 'stuffed_shells' 
    END AS specialized_type
  , CASE WHEN mc.component_type = 'Garnish' 
              AND RLIKE(mc.component_name, '.*(rub|glaze|seasoning|sauce).*')
              AND mc.container NOT IN ('N/A', 'pre-portioned')
              AND NOT RLIKE(mc.component_name, '.*(tabasco|sriracha|tomato sauce|soy sauce|marinara|red sauce|franks|glazed).*')
         THEN TRIM(REGEXP_REPLACE(mc.component_name, '[^a-z ]+', ''))
    END AS seasoning_type
  , BOOLOR_AGG(COALESCE(i.ingredient_category, '') IN ('meat', 'fish')
               AND ((mc. term_id <= 200 AND COALESCE(mc.container, '') NOT IN ('2 oz cup', '1 oz cup', 'dry sachet'))
                     OR (mc.term_id > 200 AND COALESCE(component_type, '') = 'Component 1'))) AS is_main_protein
FROM meal_components mc
LEFT JOIN {{ table_reference('mise_part_pgs') }} mpg 
  ON mc.component_id = mpg.part_id
LEFT JOIN {{ ref('ingredients') }} i
  ON mpg.purchased_good_id = i.purchased_good_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
--Note: component_weight can be 0 (water) and for some reason tomato sauce once
  --meal_sku_id can be null where production was for non-menu meals
  --container is null when the meals have partially deleted components in Misevala