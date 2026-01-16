
SELECT 
  p.id AS payment_id
  , p.status AS payment_status
  , p.userid AS customer_id
  , p.created AS payment_time
  , p.error_details IS NOT NULL AS had_error
  , p.notes AS payment_notes
  , p.stripe_charge_id
  , p.stripe_customer_id
FROM {{ table_reference('payment') }} p
INNER JOIN {{ ref('customers') }} c ON p.userid = c.customer_id
