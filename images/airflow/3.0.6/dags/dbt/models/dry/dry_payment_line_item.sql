{{ dry_config('payment_line_item') }}

SELECT
  {{ clean_string('coupon_code') }} AS coupon_code
  , {{ clean_string('coupon_code_applied_id') }} AS coupon_code_applied_id
  , created
  , {{ clean_string('id') }} AS id
  , invoice_amount_cents
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('referral_code') }} AS referral_code
  , REPLACE({{ clean_string('stripe_charge_id') }}, '\n', '') AS stripe_charge_id
  , transaction_amount_cents
  , {{ clean_string('type') }} AS type
  , updated
FROM {{ source('combined_api_v3', 'payment_line_item') }}

{{ load_incrementally() }}
