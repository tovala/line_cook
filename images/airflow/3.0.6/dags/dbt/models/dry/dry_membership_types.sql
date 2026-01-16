{{ dry_config('membership_types') }}

SELECT
  active
  , created
  , {{ clean_string('description') }} AS description
  , free_delivery
  , {{ clean_string('id') }} AS id
  , interval_days
  , {{ clean_string('notes') }} AS notes
  , price_cents
  , updated
FROM {{ source('combined_api_v3', 'membership_types') }}

{{ load_incrementally() }}
