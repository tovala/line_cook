SELECT 
  id AS refund_event_id
  , created AS refund_event_time
  , order_user_id AS customer_id 
  , order_id AS meal_order_id
  , customer_service_ticket_id AS cs_ticket_id
  , customer_service_user_id AS cs_agent_id
  , customer_service_email AS cs_agent_email 
  , reason AS refund_reason 
  , customer_service_remarks AS cs_remarks
  , action AS cs_action
  , {{ cents_to_usd('amount_cents') }} AS refund_amount
FROM {{ table_reference('order_refund_event') }}
