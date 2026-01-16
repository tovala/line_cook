-- TODO: Remove this and replace with shipment file as source of truth later.

SELECT
  ob.id AS meal_order_box_id
  , ob.userid AS customer_id
  , ob.orderfulfillment_id AS order_fulfillment_id
  , ob.termid AS term_id
  , CASE WHEN ob.box_type = ob.meals_count
         THEN ob.box_type 
    END AS box_size
  , CASE WHEN ob.shipment_status = 'shipped-1'
         THEN 'shipped'
         ELSE ob.shipment_status
    END AS box_status
  , ob.trackingnumber AS tracking_number 
  , ob.created AS box_ship_date
  , s.insulation_type
  , s.production_employee AS is_production_employee
  -- TO DO: delivered is always null is postgres, include once shipping file is loading
FROM {{ table_reference('orderboxes') }} ob
INNER JOIN {{ ref('customers') }} c 
  ON ob.userid = c.customer_id
LEFT JOIN {{ ref('meal_orders') }} mo
  ON ob.orderfulfillment_id = mo.meal_order_fulfillment_id
LEFT JOIN {{ table_reference('shipment_file') }} s 
  ON ob.id = s.orderfulfillmentid
  AND s.cycle_id = mo.cycle
  AND s.facility_network = mo.facility_network
