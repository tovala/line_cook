{{ dry_config('cookable_assist_recipe_reviews', primary_key = None, load_mode='table', post_hooks=[]) }}

SELECT
  {{ clean_string('cookable_review_id') }} AS cookable_review_id
  , created
  , tovala_assist_recipe_id
FROM {{ source('combined_api_v3', 'cookable_assist_recipe_reviews') }}