{{ dry_config('user_subscription_break_history') }}

SELECT
  {{ clean_string('action') }} AS action 
  , {{ clean_string('break_id') }} AS break_id
  , created  
  , end_term
  , {{ clean_string('id') }} AS id
  , start_term
  , user_id
--TODO: Switch off CAPI dev
FROM {{ source('combined_api_v3', 'user_subscription_break_history') }}

{{ load_incrementally(bookmark='created') }}