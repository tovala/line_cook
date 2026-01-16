{{ dry_config('user_subscription_breaks') }}

SELECT
  created
  , end_term
  , {{ clean_string('id') }} AS id
  , start_term
  , updated
  , user_id
FROM {{ source('combined_api_v3', 'user_subscription_breaks') }}

{{ load_incrementally() }}