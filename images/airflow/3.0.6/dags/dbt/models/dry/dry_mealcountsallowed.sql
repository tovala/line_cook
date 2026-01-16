{{ dry_config('mealcountsallowed') }}

SELECT
  active
  , TRY_PARSE_JSON(box_sizes) AS box_sizes
  , {{ clean_string('description') }} AS description
  , {{ clean_string('id') }} AS id
  , meal_count
  , {{ clean_string('name') }} AS name
  , price_cents
  , shipping_cents
  , updated
FROM {{ source('combined_api_v3', 'mealcountsallowed') }}

{{ load_incrementally() }}
