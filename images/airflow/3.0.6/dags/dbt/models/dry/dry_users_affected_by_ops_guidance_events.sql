{{ dry_config('users_affected_by_ops_guidance_events')}}

SELECT
  id
  , {{ clean_string('event_id') }} AS event_id
  , user_id
  , {{ clean_string('extra_info') }} AS extra_info
  , created
FROM {{ source('combined_api_v3', 'users_affected_by_ops_guidance_events')}}

{{ load_incrementally(bookmark='created') }}