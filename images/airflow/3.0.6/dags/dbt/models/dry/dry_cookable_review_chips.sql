{{ dry_config('cookable_review_chips') }}

SELECT
  created 
  , display_order 
  , id 
  , {{ clean_string('name') }} AS name 
  , {{ clean_string('parent') }} AS parent 
  , {{ clean_string('sentiment') }} AS sentiment 
  , tags 
  , updated
FROM {{ source('combined_api_v3', 'cookable_review_chips') }}

{{ load_incrementally() }}
