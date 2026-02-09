with data_pull AS (
--ovens
SELECT
  c.customer_id AS customer_number
  , null AS billing_first_name
  , null AS billing_last_name
  , null AS billing_company_name
  , null AS billing_primary_address
  , null AS billing_xline_address
  , null AS billing_city
  , null AS billing_state
  , null AS billing_zipcode
  , null AS shipping_first_name
  , os.destination_name AS shipping_last_name
  , null AS shipping_company_name
  , os.destination_address_line1 AS shipping_primary_address
  , os.destination_address_line2 AS shipping_xline_address
  , os.destination_city AS shipping_city
  , os.destination_state AS shipping_state
  , os.destination_zip_cd AS shipping_zipcode
  , c.email AS email_address
  , oo.oven_order_id AS order_number
  , oo.order_time AS date_of_activity
  , oo.oven_purchase_price AS line_item_demand
  , 1 AS units
  , bpr.first_exposure_source AS channel_indicator
  , oo.oven_order_sku AS sku
  , 'oven order' AS item_description
  , null AS size
  , 'oven' AS category
  , null AS sub_category
FROM grind.customers c
INNER JOIN grind.oven_orders oo
  ON c.customer_id = oo.customer_id
INNER JOIN grind.oven_shipments os
  ON oo.oven_order_id = os.oven_order_id
LEFT JOIN grind.big_picture_responses bpr
  ON c.customer_id = bpr.customer_id
  AND bpr.nth = 1
WHERE NOT c.is_internal_account AND NOT c.is_employee
  AND oo.status IN ('refunded', 'backordered', 'complete')

UNION

--meals
SELECT
  c.customer_id AS customer_number
  , null AS billing_first_name
  , null AS billing_last_name
  , null AS billing_company_name
  , null AS billing_primary_address
  , null AS billing_xline_address
  , null AS billing_city
  , null AS billing_state
  , null AS billing_zipcode
  , null AS shipping_first_name
  , ms.destination_name AS shipping_last_name
  , null AS shipping_company_name
  , ms.destination_address_line1 AS shipping_primary_address
  , ms.destination_address_line2 AS shipping_xline_address
  , ms.destination_city AS shipping_city
  , ms.destination_state AS shipping_state
  , ms.destination_zip_cd AS shipping_zipcode
  , c.email AS email_address
  , mo.meal_order_id AS order_number
  , mo.order_time AS date_of_activity
  , cts.meal_arr AS line_item_demand
  , mo.order_size AS units
  , bpr.first_exposure_source AS channel_indicator
  , mo.term_id::STRING AS sku
  , 'meals order' AS item_description
  , null AS size
  , 'meals' AS category
  , null AS sub_category
FROM grind.customers c
INNER JOIN grind.meal_orders mo
  ON c.customer_id = mo.customer_id
INNER JOIN grind.meal_shipments ms
  ON mo.meal_order_id = ms.meal_order_id
LEFT JOIN season.customer_term_summary cts
  ON mo.meal_order_id = cts.meal_order_id
LEFT JOIN grind.big_picture_responses bpr
  ON c.customer_id = bpr.customer_id
  AND bpr.nth = 1
WHERE NOT c.is_internal_account AND NOT c.is_employee
)
SELECT *
-- DISTINCT customer_number, email_address
FROM data_pull
WHERE date_of_activity >= '%(start_date)s'::DATE
  AND date_of_activity < '%(end_date)s':::DATE;