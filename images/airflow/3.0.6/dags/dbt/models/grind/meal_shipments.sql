-- TODO: Replace this with proper source of truth as soon as shipment file are in


WITH order_boxes AS (
  SELECT 
    order_fulfillment_id,
    COUNT(DISTINCT tracking_number) AS shipped_boxes
  FROM {{ ref('meal_order_boxes') }}
  WHERE tracking_number IS NOT NULL
  GROUP BY 1
)
SELECT
  orf.id AS shipment_id
  , mo.meal_order_id
  , 'meal' AS product_type
  , orf.delivered AS delivered_time
  , mo.customer_id
  , orf.orderstatus AS status
  , orf.meal_count AS order_size
  , z.estimated_delivery_days
  , DATEADD(DAY, z.estimated_delivery_days, subterm_ship_time) AS estimated_delivery_date
  , mo.cycle 
  , orf.trackingnumber AS fulfillment_tracking_number
  , orf.termid AS term_id
  , orf.shipping_service AS shipping_service
  , orf.shipping_company AS shipping_company
  , orf.address_validation_status AS address_validation_status
  , orf.address_rdi AS destination_address_type
  , orf.ship_origin AS ship_origin
  , orf.shipping_name AS destination_name
  , orf.shipping_address_line1 AS destination_address_line1
  , orf.shipping_address_line2 AS destination_address_line2
  , orf.shipping_city AS destination_city
  , CASE
        WHEN orf.shipping_state = 'Georgia' THEN 'GA'
        WHEN orf.shipping_state IN ('New York', 'MY') THEN 'NY'
        WHEN orf.shipping_state = 'Hawaii' THEN 'HI'
        WHEN orf.shipping_state = 'I' THEN 'IL'
        WHEN lower(orf.shipping_state) = 'jacksonville' THEN 'FL' -- Yep, Jacksonville. Famously a state that we ship to. Nothing to look at here.
        WHEN orf.id = 'befa5787-7e69-8517-eeff-fbade68ff46d'
        THEN 'IN' -- Don't ask
        ELSE SUBSTR(UPPER(orf.shipping_state), 1, 2) 
    END AS destination_state
  , orf.shipping_zip AS destination_zip_cd
  , orf.shipping_phone AS destination_phone
  , CASE WHEN fulfillment_tracking_number IS NOT NULL
         THEN 1 -- the fulfillment_tracking_number is only present when there are no records in orderboxes
         WHEN ob.shipped_boxes IS NOT NULL
         THEN ob.shipped_boxes
         WHEN orf.orderstatus IN ('paid', 'refunded')
         THEN 1
         ELSE 0
    END AS shipped_boxes
  , COALESCE(pli.invoice_amount, 0) AS shipping_charge
  , pli.payment_line_item_id IS NULL AS had_free_shipping
  -- TO DO: find better value for shipment time than creation of fulfillment
  , orf.created AS shipment_time
FROM {{ table_reference('orderfulfillment') }} orf
LEFT JOIN {{ ref('terms') }} t 
  ON t.term_id = orf.termid
INNER JOIN {{ ref('meal_orders') }} mo 
  ON orf.id = mo.meal_order_fulfillment_id
LEFT JOIN {{ table_reference('zipcodes') }} z 
  ON orf.shipping_zip = z.zip
LEFT JOIN order_boxes ob 
  ON orf.id = ob.order_fulfillment_id
LEFT JOIN {{ ref('payment_line_items') }} pli 
  ON mo.payment_id = pli.payment_id
  AND pli.payment_line_item_type = 'shipping'
LEFT JOIN {{ ref('subterms') }} st
  ON mo.subterm_id = st.subterm_id