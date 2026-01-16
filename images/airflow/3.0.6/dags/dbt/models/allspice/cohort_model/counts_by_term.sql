{{
  config(
    tags=['cohort_model'],
  )
}}

SELECT
    term_id
    , cohort
    , COUNT(meal_order_id) AS order_count
    , SUM(order_size) AS meal_count
FROM {{ ref('customer_term_summary') }}
WHERE is_fulfilled
GROUP BY 1,2
