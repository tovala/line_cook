
SELECT 
  status_id
  , customer_id
  , status_start_time
  , user_status
  , prev_user_status
  , LEAD(status_start_time) OVER (PARTITION BY customer_id
                                  ORDER BY status_start_time) AS status_end_time
  , (prev_user_status IS NULL) AS is_first_status
FROM {{ ref('full_user_statuses') }}
WHERE user_status <> COALESCE(prev_user_status, '')
