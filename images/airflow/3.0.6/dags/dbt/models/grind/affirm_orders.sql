
SELECT 
  ex.id AS charge_id
  , af.id AS internal_affirm_charge_id
  , af.ovenorderid AS oven_order_fulfillment_id
  , af.userid AS customer_id
  , ex.created AS order_time
  , ex.refund_expires AS refund_expires_time
  , REPLACE(ex.status, ' ', '_') AS affirm_status
  , {{ current_timestamp_utc() }} <= refund_expires_time AND ex.refundable AS is_refundable
  , {{ cents_to_usd('ex.shipping_amount_cents') }} AS shipping_amount
  , {{ cents_to_usd('ex.amount_cents') }} AS total_charge_amount
  , -1*{{ cents_to_usd('ex.refunded_amount_cents') }} AS total_refund_amount
  , ex.shipping_email
  , ex.shipping_phone_number
  , ex.shipping_zipcode AS shipping_zip_cd
  , ex.shipping_state
FROM {{ source('brine', 'affirm_charges_external') }} ex
LEFT JOIN {{ table_reference('affirm_charges') }} af 
  ON ex.id = af.chargeid
WHERE ex.row_num = 1
