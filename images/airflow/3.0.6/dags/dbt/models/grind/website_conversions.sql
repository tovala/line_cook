
SELECT 
  --There are 65 duplicate oven_order_fulfillment_id row events as of 2/1/24, using distinct to remove
  DISTINCT
  s.session_id
  , s.fullstory_user_id
  , fe.eventstart AS oven_order_time
  , fe.evt_order_id_str AS oven_order_fulfillment_id --primary key
  , s.customer_id
FROM {{ ref('sessions') }} s
INNER JOIN {{ table_reference('fullstory_events') }} fe
  ON s.session_id = fe.sessionid
WHERE fe.eventcustomname = 'Order Completed'
  --Exlude events where the order id is not filled out or is for meals only flow
  AND COALESCE(fe.evt_order_id_str,'unknown') NOT IN ('T0000001','T12345','unknown')
--Note: you can order multiple ovens within one session (typically canceled/refunded)
