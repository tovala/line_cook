
WITH refund_amounts AS
(SELECT 
   payment_id
   , SUM(refund_amount) AS refund_cents
   , MIN(refund_time) AS refund_time
   , LISTAGG(refund_notes, ' | ') AS refund_notes
 FROM {{ ref('refunds') }}
 GROUP BY 1)
SELECT 
  r.payment_id
  , r.refund_cents + c.tax_amount + c.gift_card_amount + COALESCE(pc.post_sale_gift_card_amount, 0)
    + c.meal_credit_amount + COALESCE(pc.post_sale_meal_credit_amount, 0) 
    + c.meal_cash_amount + COALESCE(pc.post_sale_meal_cash_amount, 0) AS gross_refund_amount
  , -1*(c.meal_credit_amount + COALESCE(pc.post_sale_meal_credit_amount, 0)) AS meal_credit_refund_amount
  , -1*(c.meal_cash_amount + COALESCE(pc.post_sale_meal_cash_amount, 0)) AS meal_cash_refund_amount
  , -1*(c.gift_card_amount + COALESCE(pc.post_sale_gift_card_amount, 0)) AS gift_card_refund_amount
  , -c.tax_amount AS tax_refund_amount
  , r.refund_cents AS refund_amount  
  , r.refund_time 
  , r.refund_notes
FROM refund_amounts r 
INNER JOIN {{ ref('agg_charges') }} c ON r.payment_id = c.payment_id
LEFT JOIN {{ ref('agg_post_charges') }} pc ON r.payment_id = pc.payment_id