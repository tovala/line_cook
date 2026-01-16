{{ dry_config('ovenorderfulfillment') }}

SELECT
  {{ clean_string('coupon_code') }} AS coupon_code
  , created
  , delivered
  , {{ clean_string('fedex_order_id') }} AS fedex_order_id
  , {{ clean_string('id') }} AS id
  , metadata
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('orderstatus') }} AS orderstatus
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , {{ clean_string('referral_code') }} AS referral_code
  , {{ clean_string('shippingaddress') }} AS shippingaddress
  , {{ clean_string('sku') }} AS sku
  , {{ clean_string('stripe_customerid') }} AS stripe_customerid
  , {{ clean_string('stripe_orderid') }} AS stripe_orderid
  , {{ clean_string('trackingnumber') }} AS trackingnumber
  , updated
  , userid
FROM {{ source('combined_api_v3', 'ovenorderfulfillment') }}

{{ load_incrementally() }}
