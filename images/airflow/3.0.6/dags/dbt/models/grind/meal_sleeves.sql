
WITH clean_tags AS (
  SELECT
    id
    , parse_json(tags):tags AS tags
  FROM {{ table_reference('mise_production_parts') }}
), clean_tags_flattened AS (
  SELECT 
    ct.id
    , ct.tags
    , VALUE:category::string AS cat
    , VALUE:title::string AS val
  FROM clean_tags ct, LATERAL FLATTEN (input => tags)
)
, meal_sleeves AS (
  SELECT 
    mpp.id
    , mpp.cycle
    , mfn.title AS facility_network
    , mpmi.api_meal_code AS production_cd
    , mpmi.term_id
    , MAX(CASE WHEN ctf.cat = 'sleeving_location' THEN ctf.val END) AS sleeving_location
    , MAX(CASE WHEN ctf.cat = 'sleeving_configuration' THEN ctf.val END) AS sleeving_configuration
  FROM {{ table_reference('mise_production_parts') }} mpp
  INNER JOIN {{ table_reference('mise_production_meal_info') }} mpmi
    ON mpp.id = mpmi.production_part_id
    AND mpp.facility_network_id = mpmi.facility_network_id
    AND mpp.cycle = mpmi.cycle
  LEFT JOIN {{ table_reference('mise_facility_network') }} mfn
    ON mpp.facility_network_id = mfn.id
  LEFT JOIN clean_tags_flattened ctf
    ON mpp.id = ctf.id
  WHERE mpmi.api_meal_code IS NOT NULL --not null for meals
  GROUP BY 1,2,3,4,5
)
SELECT
  ms.id
  , ms.cycle
  , ms.facility_network
  , ms.production_cd
  , ms.term_id
  , ms.sleeving_location
  --As of 14NOV22, there should only be 3 sleeving categories (Drop Shelf, Open Garnish, and Short Boi)
  , CASE WHEN ms.sleeving_configuration = 'NGT Drop Shelf' THEN 'Drop Shelf'
         WHEN ms.sleeving_configuration = 'Standard' THEN 'Open Garnish'
         ELSE ms.sleeving_configuration
    END AS sleeving_configuration
  FROM meal_sleeves ms
