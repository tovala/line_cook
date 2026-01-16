{{ dry_config('barcodes') }}

SELECT
  {{ clean_string('barcode') }} AS barcode
  , created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('name') }} AS name
  , TRY_PARSE_JSON(routine) AS routine
  , updated
  , userid
  , {{ clean_string('user_custom_meal_id') }} AS user_custom_meal_id
FROM {{ source('combined_api_v3', 'barcodes') }}

{{ load_incrementally() }}
