{{ dry_config('coupon_codes_applied') }}

SELECT
  active
  , {{ clean_string('coupon_code') }} AS coupon_code
  , date_redeemed
  , expiration
  , {{ clean_string('id') }} AS id
  , updated
  , userid
  , {{ clean_string('referral_code') }} AS referral_code
FROM {{ source('combined_api_v3', 'coupon_codes_applied') }}

{{ load_incrementally() }}
