{{ dry_config('mealreviews') }}

SELECT
  {{ clean_string('comment') }} AS comment
  , created
  , {{ clean_string('id') }} AS id
  , mealid
  , {{ clean_string('medium') }} AS medium
  , rating
  , {{ clean_string('side_comment') }} AS side_comment
  , side_rating
  , updated
  , userid
  , valid
FROM {{ source('combined_api_v3', 'mealreviews') }}

{{ load_incrementally() }}
