SELECT DISTINCT
  mi.mealid AS meal_sku_id
  , mmo.internal_meal_name
  , mi.caption
  , mi.key 
  , mi.filename AS file_name
  , CONCAT('https:', mi.url) AS url
  , mi.updated
  , CASE WHEN REGEXP_SUBSTR(LOWER(mi.filename), '__new') IS NOT NULL THEN 'new'
         WHEN REGEXP_SUBSTR(LOWER(mi.filename), '__old') IS NOT NULL THEN 'old'
    END AS photo_type
  , ROW_NUMBER() OVER (PARTITION BY mi.mealid, mi.key ORDER BY mi.updated DESC) = 1 AS is_latest_image
FROM {{ table_reference('mealimages') }} mi
LEFT JOIN {{ ref('menu_meal_offerings') }} mmo
  ON mi.mealid = mmo.meal_sku_id
