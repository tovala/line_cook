
SELECT 
  mns.internal_meal_name
  , mns.meal_id
  , mns.term_id
  , mns.cycle
  , mfn.title AS facility_network_name
  , mns.production_cd
  , ms.meal_sku_id
  , mpmi.total_meals AS meals_produced_count
FROM {{ ref('meal_name_standardizations') }} mns
LEFT JOIN {{ table_reference('mise_production_meal_info') }} mpmi
  ON mns.production_cd = mpmi.api_meal_code
  AND mns.term_id = mpmi.term_id
  AND mns.cycle = mpmi.cycle
  AND mns.facility_network_id = mpmi.facility_network_id
LEFT JOIN {{ table_reference('mise_facility_network') }} mfn
  ON mns.facility_network_id = mfn.id
LEFT JOIN {{ ref('meal_skus') }} ms
  ON mns.production_cd = ms.production_cd
  AND mns.term_id = ms.term_id
  AND CONTAINS(ms.facility_network, mfn.title) --facility_network in meal_skus is concatenated
  AND mns.meal_id = ms.meal_id
WHERE mpmi.total_meals > 0 --if none were produced that cycle, remove from query
  AND ms.meal_sku_id IS NOT NULL