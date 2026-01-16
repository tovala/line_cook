{{ dry_config('cookable_reviews') }}

SELECT
  {{ clean_string('comment') }} AS comment 
  , created 
  , id 
  , {{ clean_string('platform') }} AS platform 
  , {{ clean_string('rating_type') }} AS rating_type 
  , updated 
  , userid
  , rating
  , rating_int
  , {{ clean_string('review_type') }} AS review_type
FROM {{ source('combined_api_v3', 'cookable_reviews') }}

{{ load_incrementally() }}
