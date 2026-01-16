
SELECT
  id AS rtg_customers_id
  , event_id AS ops_guidance_event_id
  , user_id AS customer_id
  , extra_info
  , created AS created_time
FROM {{ table_reference('users_affected_by_ops_guidance_events')}}
