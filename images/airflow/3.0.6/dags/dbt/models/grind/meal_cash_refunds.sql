
SELECT 
  payment_id
  , customer_id
  , SUM(meal_cash_amount)*-1 AS refund_amount
  , MIN(transaction_time) AS refund_time
FROM {{ ref('meal_cash') }} 
WHERE action_category IN ('order_refund', 'order_refund_partial', 'shipping_refund')
  AND payment_id IS NOT NULL
  AND voided IS NULL
GROUP BY 1,2
-- Only include refunds that are not ultimately removed
HAVING refund_amount <> 0