{{ dry_config('mealtagmap') }}

SELECT
  mealid || ',' || tagid AS id
  , display_order
  , mealid
  , TRY_PARSE_JSON(meal_tag_metadata) AS meal_tag_metadata
  , tagid
  , updated
FROM {{ source('combined_api_v3', 'mealtagmap') }}

{{ load_incrementally() }}
