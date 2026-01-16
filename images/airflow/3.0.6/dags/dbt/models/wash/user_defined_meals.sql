
WITH barcode_dedupe AS 
(SELECT DISTINCT 
   id AS barcode_id
   , UPPER(barcode) AS barcode
   , userid AS customer_id 
   , UPPER(user_custom_meal_id) AS user_custom_meal_id
   , created AS barcode_created_time
   , ROW_NUMBER() OVER (PARTITION BY UPPER(barcode), customer_id ORDER BY created DESC) AS row_num
 FROM {{ table_reference('barcodes') }} 
 WHERE userid IS NOT NULL
), parsed_recipes AS (
  SELECT DISTINCT
    userid AS customer_id 
    , {{ clean_string('UPPER(value:"barcode"::STRING)') }} AS barcode 
    , {{ clean_string('value:"title"::STRING') }} AS recipe_title
    , value:"isFavorite"::BOOLEAN AS is_favorite
    , CASE WHEN TO_TIMESTAMP_TZ(value:"creationDate")::DATE < '2017-07-01'
           THEN created 
           WHEN TO_TIMESTAMP_TZ(value:"creationDate")::DATE > {{ current_timestamp_utc() }}::DATE 
           THEN updated
           ELSE TO_TIMESTAMP_TZ(value:"creationDate") 
      END AS recipe_created_time
  FROM {{ table_reference('usermealdatajson') }}, LATERAL FLATTEN(INPUT => data:"userRecipes") AS recipes
  WHERE RLIKE({{ clean_string('UPPER(value:"barcode"::STRING)') }}, '(MAPP-|ROBO-|TEMP-|MAPP-IOS-|MAPP-TEMP-|)\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}')
)
SELECT 
  COALESCE(cur.customer_id, udb.customer_id) AS customer_id
  , cur.barcode AS custom_meal_id
  , cur.recipe_title 
  , cur.is_favorite 
  , cur.recipe_created_time 
  , udb.barcode AS alternate_barcode
  , udb.barcode_created_time AS alternate_start_time
  , COALESCE(LEAD(udb.barcode_created_time) 
             OVER (PARTITION BY cur.customer_id, cur.barcode 
                   ORDER BY alternate_start_time)
    , '9999-12-31') AS alternate_end_time
FROM parsed_recipes cur 
FULL OUTER JOIN barcode_dedupe udb 
  ON cur.barcode = udb.user_custom_meal_id 
  AND cur.customer_id = udb.customer_id
WHERE COALESCE(udb.row_num, 1) = 1
