
SELECT 
  cts.customer_id 
  , cts.term_id 
  , mo.order_size  
  , ra.order_value
  , EXTRACT(MONTH FROM cts.cohort_start_date) AS cohort_month
  , EXTRACT(YEAR FROM cts.cohort_start_date) AS cohort_year
  , EXTRACT(MONTH FROM te.start_date) AS term_month
  , EXTRACT(YEAR FROM te.start_date) AS term_year
FROM {{ ref('customer_term_summary') }} cts
LEFT JOIN {{ ref('meal_orders') }} mo 
  ON cts.meal_order_id = mo.meal_order_id 
LEFT JOIN {{ ref('revenue_aggregations') }} ra 
  ON cts.meal_order_id = ra.order_id 
LEFT JOIN {{ ref('terms') }} te 
  ON cts.term_id = te.term_id
WHERE cts.is_fulfilled 
  AND NOT cts.is_internal_account
  AND cts.term_id >= 157
