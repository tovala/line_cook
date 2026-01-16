
SELECT 
  ps.id AS product_shipment_id
  , pp.id AS oven_order_id
  , ps.userid AS customer_id 
  , ps.order_status AS exchange_shipment_status 
  , ps.fulfillment_service
  , ps.shipping_order_id
  , ps.shipping_name AS destination_name
  , ps.tracking_number
  , pp.created AS oven_order_time
  , ps.created AS oven_shipment_time
  , ps.updated AS oven_shipment_updated_time
FROM {{ table_reference('products_shipment') }} ps
LEFT JOIN {{ table_reference('products_purchase') }} pp 
  ON pp.id = ps.products_purchase_id
WHERE ps.product_type = 'exchange-oven'
