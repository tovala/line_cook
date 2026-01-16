
WITH meal_name_standardizations AS (
  --Cleanup title names
  SELECT mpp.part_id
    ,  CASE WHEN LEFT(mpp.part_title,3) = 'QVC' THEN LTRIM(REGEXP_REPLACE(TRIM(mpp.part_title), '( ){2,}',' '),'QVC ') --remove prefix of QVC
            ELSE REGEXP_REPLACE(TRIM(mpp.part_title), '( ){2,}',' ')
            END AS cleaned_title
    , mpp.term_id
    , mpp.cycle
    , mpp.facility_network_id
    , mpmi.api_meal_code AS production_cd
    , mpp.part_version_id as version_id
    , LAST_VALUE(cleaned_title) OVER (PARTITION BY mpp.part_id ORDER BY mpp.term_id) AS internal_meal_name
  FROM {{ table_reference('mise_production_parts') }} mpp
  LEFT JOIN {{ table_reference('mise_production_meal_info') }} mpmi --only contains information about the meal
    ON mpp.id = mpmi.production_part_id
    AND mpp.facility_network_id = mpmi.facility_network_id
    AND mpp.cycle = mpmi.cycle
  INNER JOIN {{ ref('terms') }} t --only keep valid terms
    ON mpp.term_id = t.term_id
  WHERE mpp.is_meal
    AND LOWER(mpp.part_title) NOT LIKE '%test%' --remove any tests
    AND mpmi.api_meal_code IS NOT NULL --if the api_meal_title is not there, then it was not an actual meal (ex: bbq sauce)
)
SELECT 
  mns.internal_meal_name
  , LAST_VALUE(mns.part_id) OVER (PARTITION BY mns.internal_meal_name ORDER BY mns.term_id) AS meal_id --primary key, most current part_id
  , mns.part_id --to facilitate joining back onto production_parts, this value should be the same per meal after Apr 2021
  , mns.version_id --to facilitate joining back onto production_parts
  , LAST_VALUE(mns.version_id) OVER (PARTITION BY mns.internal_meal_name ORDER BY mns.term_id) AS current_version_id
  , mns.term_id
  , mns.cycle
  , mns.facility_network_id
  , mns.production_cd
  , mfn.title AS facility_network_name
FROM meal_name_standardizations mns
LEFT JOIN {{ table_reference('mise_facility_network') }} mfn
  ON mns.facility_network_id = mfn.id
