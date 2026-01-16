SELECT
  {{ hash_natural_key('status_id', 'customer_id') }} AS customer_status_change_id
  , status_id 
  , customer_id 
  , status_start_time 
  , status_end_time 
  , user_status AS customer_status 
  , CASE WHEN status_end_time IS NULL THEN TRUE ELSE FALSE END AS is_current_customer_status
FROM {{ ref('full_user_statuses') }}
