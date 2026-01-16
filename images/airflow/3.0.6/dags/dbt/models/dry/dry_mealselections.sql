{{ dry_config('mealselections') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , mealid
  , selected_automatically
  , termid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'mealselections') }}

{{ load_incrementally() }}
