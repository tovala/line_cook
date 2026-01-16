
WITH order_statuses AS (  
  SELECT 
    userid
    , termid 
    , status
    , ROW_NUMBER() OVER (PARTITION BY userid ORDER BY CREATED DESC) AS row_num 
  FROM {{ table_reference('user_term_order') }}
  WHERE status NOT IN ('on_break', 'skipped'))
SELECT 
  userid AS customer_id
FROM order_statuses
WHERE status = 'payment_delinquent'
  AND row_num = 1