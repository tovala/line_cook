
SELECT 
  mo.id AS rental_oven_payment_id 
  , mo.userid  AS customer_id 
  , mo.termid AS term_id
  , mo.membership_id 
  , mo.payment_id
  , mo.status AS order_status 
  , mo.notes AS order_notes
  , m.membership_type_id
  , mo.created AS payment_time
-- TO DO: clean memberships table rather than pulling from raw
FROM {{ table_reference('membership_orders') }} mo
INNER JOIN {{ table_reference('memberships') }} m ON mo.membership_id = m.id
INNER JOIN {{ ref('customers') }} c ON mo.userid = c.customer_id
