
SELECT 
  oo.oven_order_fulfillment_id AS shipment_id
  , oo.oven_order_id 
  , oo.customer_id
  , oo.status AS order_status
  , oo.oven_order_sku
  , 'oven' AS product_type
  , CASE WHEN oo.status = 'canceled' AND COALESCE(ps.fulfillment_queue_status, '') = 'canceled'
         THEN 'canceled'
         WHEN oo.status = 'refunded' AND COALESCE(ps.fulfillment_queue_status, '') = 'canceled'
         THEN 'returned'
         WHEN oo.status = 'complete' AND COALESCE(ps.fulfillment_queue_status, '') = 'ready-to-fulfill'
         THEN 'ready_to_ship'
         WHEN oo.status = 'complete' AND COALESCE(ps.fulfillment_queue_status, '') = 'on_hold'
         THEN 'shipment_held'
         -- Historically, we don't have FQS for all completed orders (before 2022), we can safely assume they are shipped if the status is complete
         WHEN oo.status = 'complete' AND COALESCE(ps.fulfillment_queue_status, 'fulfillment-created') IN ('fulfillment-created', 'submitted-to-queue')
         THEN 'shipped'
    END AS shipment_status
  , ps.fulfillment_queue_status
  , ps.fulfillment_service AS three_pl_provider
  --Shipment generally takes 3 business days
  , CASE WHEN DAYOFWEEK(CONVERT_TIMEZONE('America/Chicago', oof.created)) IN (1,2,3,4) 
         THEN oof.created + INTERVAL '3 days'
         WHEN DAYOFWEEK(CONVERT_TIMEZONE('America/Chicago', oof.created)) = 5 AND HOUR(CONVERT_TIMEZONE('America/Chicago', oof.created)) <= 17 
         THEN oof.created + INTERVAL '3 days'
         WHEN DAYOFWEEK(CONVERT_TIMEZONE('America/Chicago', oof.created)) = 5 AND HOUR(CONVERT_TIMEZONE('America/Chicago', oof.created)) > 17 
         THEN oof.created + INTERVAL '6 days'
         WHEN DAYOFWEEK(CONVERT_TIMEZONE('America/Chicago', oof.created)) = 6 
         THEN oof.created + INTERVAL '5 days'
         WHEN DAYOFWEEK(CONVERT_TIMEZONE('America/Chicago', oof.created)) = 0 
         THEN oof.created + INTERVAL '4 days'
    END AS estimated_delivery_time
  , fedex_order_id
  , COALESCE(ps.tracking_number, oof.trackingnumber) AS tracking_number
  , UPPER(ps.shipping_service) AS shipping_company 
  , ps.shipping_name AS destination_name
  , ps.shipping_address_line1 AS destination_address_line1                        
  , ps.shipping_address_line2 AS destination_address_line2
  , COALESCE(ps.shipping_city, 
             SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(shipping_address), CHR(10), 1)), ',', 1)) AS destination_city
  , UPPER(COALESCE(ps.shipping_state,
             SPLIT_PART(REGEXP_SUBSTR(ps.shipping_address, '[[:upper:]]{2}\\s[[:digit:]]{5}$'), ' ', 1))) AS destination_state 
  , COALESCE(ps.shipping_zip, 
             REGEXP_SUBSTR(TRIM(ps.shipping_address), '[[:digit:]]{5}$')) AS destination_zip_cd
  , COALESCE(ps.shipping_phone, 
             REGEXP_SUBSTR(TRIM(ps.shipping_address), '[[:digit:]]{10}')) AS destination_phone
  -- TO DO: find better value for shipment time than creation of fulfillment
  , COALESCE(ps.created, oof.created) AS shipment_time 
FROM {{ ref('oven_orders') }} oo
INNER JOIN {{ table_reference('ovenorderfulfillment') }} oof 
  ON oof.id = oo.oven_order_fulfillment_id 
LEFT JOIN {{ table_reference('products_shipment') }} ps 
  ON ps.id = oo.products_shipment_id
