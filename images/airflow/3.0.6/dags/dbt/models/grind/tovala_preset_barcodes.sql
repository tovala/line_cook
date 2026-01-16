
SELECT DISTINCT 
  UPPER({{ clean_string('value:identifier::STRING') }}) AS barcode 
  , {{ clean_string('value:title::STRING') }} AS title 
  , {{ clean_string('value:catagory::STRING') }} AS preset_category
  , value:isUserVisible::BOOLEAN AS is_user_visible 
  , COALESCE(value:miniRoutine, value:routine) AS cook_routine
  , LOWER({{ clean_string('value:userCatagory::STRING') }}) AS user_category
  , filename = 'presets/presets.json' AS is_live
FROM {{ source('chili', 'tovala_preset') }}, LATERAL FLATTEN(raw_data:presets)
-- this file has a different format from the other files and no unique information
WHERE filename <> 'presets/presets.json.38'
QUALIFY ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY raw_data:version DESC) = 1
