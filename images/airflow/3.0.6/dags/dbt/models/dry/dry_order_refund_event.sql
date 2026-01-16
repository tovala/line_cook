{{ dry_config('order_refund_event') }}

SELECT
  {{ clean_string('id') }} AS id 
  , {{ clean_string('order_id') }} AS order_id 
  , {{ clean_string('order_user_id') }} AS order_user_id
  , amount_cents 
  , {{ clean_string('reason') }} AS reason 
  , customer_service_user_id 
  , {{ clean_string('customer_service_email') }} AS customer_service_email 
  , {{ clean_string('customer_service_ticket_id') }} AS customer_service_ticket_id 
  , {{ clean_string('customer_service_remarks') }} AS customer_service_remarks
  , {{ clean_string('action') }} AS action
  , created AS created
FROM {{ source('combined_api_v3', 'order_refund_event') }}

{{ load_incrementally(bookmark='created') }}
