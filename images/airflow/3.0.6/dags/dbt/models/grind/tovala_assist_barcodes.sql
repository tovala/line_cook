
SELECT 
  UPPER({{ clean_string('value:barcode::STRING') }}) AS barcode
  , {{ clean_string('value:title::STRING') }} AS title
  , {{ clean_string('value:userHint::STRING') }} AS user_hint
  , value:id::INTEGER AS internal_id
  , {{ clean_string('value:servings::STRING') }} AS servings
  , filename = 'assist/recipes.json' AS is_live
FROM {{ source('chili', 'tovala_assist') }}, LATERAL FLATTEN(raw_data:recipes)
WHERE filename NOT ILIKE '%test%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY raw_data:version DESC) = 1
