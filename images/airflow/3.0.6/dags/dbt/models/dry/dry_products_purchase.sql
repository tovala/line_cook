{{ dry_config('products_purchase') }}

SELECT
  {{ clean_string('affirm_charge_id') }} AS affirm_charge_id
  , {{ clean_string('coupon_code') }} AS coupon_code
  , created
  , {{ clean_string('id') }} AS id
  , TRY_PARSE_JSON(metadata) AS metadata
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('ovenorderfulfillment_id') }} AS ovenorderfulfillment_id
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('products_shipment_id') }} AS products_shipment_id
  , {{ clean_string('product_id') }} AS product_id
  , {{ clean_string('referral_code') }} AS referral_code
  , {{ clean_string('third_party_purchase_id') }} AS third_party_purchase_id
  , updated
  , userid
  , valid
  , warranty_ends
  , {{ clean_string('warranty_period') }} AS warranty_period
  , warranty_unit
FROM {{ source('combined_api_v3', 'products_purchase') }}

{{ load_incrementally() }}
