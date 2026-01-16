-- For customers whose first orders were through GMA or Trialvala:
--  IF they never ordered again, include row for gma or triavala order and flag as is_excluded_order
--  IF they ordered again, include row for first non-excluded order and include first excluded term in separate column

SELECT 
  mo.*
  , fo.first_non_excluded_term IS NULL AS is_excluded_order
  -- Intentionally exclucdes NULL first_non_exclued_term
  , CASE WHEN fo.first_non_excluded_term <> fo.first_term
         THEN fo.first_term 
    END AS first_term_with_excluded
  , ms.shipping_service as first_shipping_service
  , COALESCE(p.had_error, FALSE) AS had_payment_error
FROM {{ ref('meal_orders') }} mo 
INNER JOIN (SELECT 
              customer_id
              , MIN(term_id) AS first_term 
              , MIN(CASE WHEN NOT (is_gma_order OR is_trial_order) THEN term_id END) AS first_non_excluded_term
            FROM {{ ref('meal_orders') }}
            WHERE is_fulfilled
            GROUP BY 1) fo 
ON mo.customer_id = fo.customer_id
  AND mo.term_id = COALESCE(fo.first_non_excluded_term, fo.first_term)
LEFT JOIN {{ ref('meal_shipments') }} ms
ON fo.customer_id = ms.customer_id  
  AND COALESCE(fo.first_non_excluded_term, fo.first_term) = ms.term_id
LEFT JOIN {{ ref('payments') }} p
  ON mo.payment_id = p.payment_id