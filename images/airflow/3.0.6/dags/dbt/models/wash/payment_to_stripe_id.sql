
WITH payment_to_stripe AS (
(SELECT DISTINCT
  id AS payment_id 
  , stripe_charge_id 
 FROM {{ table_reference('payment') }}
 WHERE stripe_charge_id IS NOT NULL)
UNION
(SELECT DISTINCT
  payment_id 
  , stripe_charge_id 
 FROM {{ table_reference('payment_line_item') }}
 WHERE stripe_charge_id IS NOT NULL)
)
SELECT 
  payment_id 
  , ARRAY_AGG(DISTINCT stripe_charge_id) AS stripe_charge_ids
FROM payment_to_stripe
GROUP BY 1