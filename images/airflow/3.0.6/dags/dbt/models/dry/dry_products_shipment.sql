{{ dry_config('products_shipment') }}

SELECT
  created
  , {{ clean_string('flexe_carrier_id') }} AS flexe_carrier_id
  , {{ clean_string('fulfillment_queue_status') }} AS fulfillment_queue_status
  , fulfillment_request_time
  , {{ clean_string('fulfillment_service') }} AS fulfillment_service
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('order_status') }} AS order_status
  , {{ clean_string('packslip_url') }} AS packslip_url
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , {{ clean_string('product_type') }} AS product_type
  , {{ clean_string('reason') }} AS reason
  , {{ clean_string('shipment_sku') }} AS shipment_sku
  , {{ clean_string('shipping_address') }} AS shipping_address
  , {{ clean_string('shipping_address_line1') }} AS shipping_address_line1
  , {{ clean_string('shipping_address_line2') }} AS shipping_address_line2
  , {{ clean_string('shipping_city') }} AS shipping_city
  , {{ clean_string('shipping_method') }} AS shipping_method
  , {{ clean_string('shipping_name') }} AS shipping_name
  , {{ clean_string('shipping_order_id') }} AS shipping_order_id
  , {{ clean_string('shipping_phone') }} AS shipping_phone
  , {{ clean_string('shipping_service') }} AS shipping_service
  , {{ clean_string('shipping_state') }} AS shipping_state
  , {{ clean_string('shipping_zip') }} AS shipping_zip
  , {{ clean_string('tracking_number') }} AS tracking_number
  , {{ clean_string('transaction_order_id') }} AS transaction_order_id
  , {{ clean_string('shipped_from_warehouse') }} AS shipped_from_warehouse
  , updated
  , userid
FROM {{ source('combined_api_v3', 'products_shipment') }}

{{ load_incrementally() }}
