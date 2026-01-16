{{ dry_config('third_party_purchases') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('product_id') }} AS product_id
  , real_user_matched
  , self_declared_order
  , {{ clean_string('shipping_city') }} AS shipping_city
  , {{ clean_string('shipping_line1') }} AS shipping_line1
  , {{ clean_string('shipping_line2') }} AS shipping_line2
  , {{ clean_string('shipping_name') }} AS shipping_name
  , {{ clean_string('shipping_phone') }} AS shipping_phone
  , {{ clean_string('shipping_state') }} AS shipping_state
  , {{ clean_string('shipping_zip') }} AS shipping_zip
  , {{ clean_string('status') }} AS status
  , updated
  , userid
  , {{ clean_string('vendor_customer_id') }} AS vendor_customer_id
  , {{ clean_string('vendor_order_number') }} AS vendor_order_number
  , {{ clean_string('vendor_po_number') }} AS vendor_po_number
FROM {{ source('combined_api_v3', 'third_party_purchases') }}

{{ load_incrementally() }}
