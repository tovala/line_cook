{{ dry_config('tovala_assist_recipe_reviews') }}

SELECT
  {{ clean_string('comment') }} AS comment
  , created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('medium') }} AS medium
  , rating
  , recipe_id
  , updated
  , userid
  , valid
FROM {{ source('combined_api_v3', 'tovala_assist_recipe_reviews') }}

{{ load_incrementally() }}
