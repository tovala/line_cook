{{ dry_config('users') }}

SELECT
  TRY_PARSE_JSON(admin_scope) AS admin_scope
  , {{ clean_string('email') }} AS email
  , email_verified
  , fcm_tokens
  , good_customer
  , id
  , is_employee
  , {{ clean_string('name') }} AS name
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('password') }} AS password
  , push_tokens
  , qa_mode_enable
  , {{ clean_string('referral_code') }} AS referral_code
  , registered
  , updated
FROM {{ source('combined_api_v3', 'users') }}

{{ load_incrementally() }}
