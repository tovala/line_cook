{{ dry_config('membership_orders') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('membership_id') }} AS membership_id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('status') }} AS status
  , termid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'membership_orders') }}

{{ load_incrementally() }}
