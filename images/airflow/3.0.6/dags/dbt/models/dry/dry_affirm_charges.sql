{{ dry_config('affirm_charges') }}

SELECT
  amount
  , {{ clean_string('captureid') }} AS captureid
  , {{ clean_string('chargeid') }} AS chargeid
  , {{ clean_string('chargestatus') }} AS chargestatus
  , {{ clean_string('checkout_token') }} AS checkout_token
  , {{ clean_string('coupon_code') }} AS coupon_code
  , created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('ovenorderid') }} AS ovenorderid
  , {{ clean_string('sku') }} AS sku
  , {{ clean_string('transactionid') }} AS transactionid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'affirm_charges') }}

{{ load_incrementally() }}