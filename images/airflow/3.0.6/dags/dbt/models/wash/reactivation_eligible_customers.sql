WITH latest_closed_term AS (
  SELECT 
    MAX(term_id) AS term_id
  FROM {{ ref('terms') }}
  WHERE is_past_order_by
)

SELECT DISTINCT
  cf.customer_id
FROM {{ ref('customer_term_summary') }} cts
INNER JOIN {{ ref('customer_facts') }} cf 
  ON cts.customer_id = cf.customer_id
CROSS JOIN latest_closed_term lct
WHERE cf.LATEST_STATUS IN ('active', 'paused')
  AND cts.is_reactivation_eligible
  AND cts.CUSTOMER_ID NOT IN (
    SELECT DISTINCT
      customer_id
    FROM {{ ref('future_term_summary') }}
    WHERE IS_PAST_ORDER_BY = FALSE
      AND IS_FULLY_SELECTED = TRUE
  )
  AND cf.email NOT LIKE '%@tovala.com'
  AND cf.customer_id NOT IN (
    SELECT DISTINCT
      customer_id
    FROM {{ ref('customer_term_summary') }}
    WHERE term_id > (lct.term_id -3)
      AND order_status IN ('payment_delinquent', 'payment_error')
  )