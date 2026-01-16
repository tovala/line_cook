{{ dry_config('orderfulfillment') }}

SELECT
  {{ clean_string('action') }} AS action
  , {{ clean_string('address_error') }} AS address_error
  , {{ clean_string('address_rdi') }} AS address_rdi
  , {{ clean_string('address_validation_status') }} AS address_validation_status
  , {{ clean_string('affected_term') }} AS affected_term
  , created
  , {{ clean_string('cs_agent') }} AS cs_agent
  , delivered
  , first_shipment_to_user
  , force_fail
  , {{ clean_string('id') }} AS id
  , TRY_PARSE_JSON(meals)::ARRAY AS meals
  , meal_count
  , {{ clean_string('notes') }} AS notes
  , number_affected_meals
  , {{ clean_string('orderstatus') }} AS orderstatus
  , payment_required
  , process_with_term
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , {{ clean_string('reason') }} AS reason
  , refunded_date
  , {{ clean_string('shippingaddress') }} AS shippingaddress
  , {{ clean_string('shipping_address_line1') }} AS shipping_address_line1
  , {{ clean_string('shipping_address_line2') }} AS shipping_address_line2
  , {{ clean_string('shipping_city') }} AS shipping_city
  , {{ clean_string('shipping_company') }} AS shipping_company
  , {{ clean_string('shipping_name') }} AS shipping_name
  , {{ clean_string('shipping_phone') }} AS shipping_phone
  , {{ clean_string('shipping_service') }} AS shipping_service
  , {{ clean_string('shipping_state') }} AS shipping_state
  , {{ clean_string('shipping_zip') }} AS shipping_zip
  , {{ clean_string('ship_origin') }} AS ship_origin
  , {{ clean_string('stripe_charge_id') }} AS stripe_charge_id
  , {{ clean_string('stripe_customerid') }} AS stripe_customerid
  , {{ clean_string('stripe_error') }} AS stripe_error
  , {{ clean_string('stripe_error_detail') }} AS stripe_error_detail
  , {{ clean_string('stripe_orderid') }} AS stripe_orderid
  , {{ clean_string('stripe_order_coupon') }} AS stripe_order_coupon
  , {{ clean_string('subscriptiontype') }} AS subscriptiontype
  , termid
  , {{ clean_string('trackingnumber') }} AS trackingnumber
  , updated
  , userid
  , {{ clean_string('zd_ticket_number') }} AS zd_ticket_number
FROM {{ source('combined_api_v3', 'orderfulfillment') }}

{{ load_incrementally() }}
