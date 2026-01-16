
(SELECT 
   customer_id 
   , status_id 
   , status_start_time
   , default_order_size
   , wants_double_autofill
   , on_commitment 
 FROM (SELECT
         customer_id
         , status_id 
         , status_start_time
         , default_order_size
         , wants_double_autofill
         , on_commitment
         , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY status_start_time) AS row_num
       FROM {{ ref('full_user_statuses') }} 
       WHERE user_status = 'active')
 WHERE row_num = 1) 
UNION
-- This captures the ~153 users who don't have a first active status but DO have a fulfilled meal order
(SELECT 
   fu.customer_id
   , fu.status_id
   , fu.status_start_time
   , fu.default_order_size
   , fu.wants_double_autofill
   , fu.on_commitment
 FROM (SELECT 
         fus.customer_id
         , SUM(CASE WHEN user_status = 'active' THEN 1 ELSE 0 END) AS active_rows
       FROM {{ ref('full_user_statuses') }}  fus
       INNER JOIN {{ ref('first_meal_orders') }}  fmo 
       ON fus.customer_id = fmo.customer_id
       GROUP BY 1) AS c
 INNER JOIN {{ ref('full_user_statuses') }}  fu 
 ON c.customer_id = fu.customer_id
   AND fu.prev_user_status IS NULL
   AND c.active_rows = 0)