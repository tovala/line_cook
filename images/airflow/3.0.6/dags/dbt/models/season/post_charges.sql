
SELECT 
  pl.payment_line_item_id AS post_charge_id
  , pl.payment_id
  , pl.payment_line_item_type AS post_charge_type
  , pl.transaction_amount AS post_charge_amount
  , pl.payment_line_item_notes AS post_charge_notes
  , pl.payment_time AS post_charge_time
FROM {{ ref('payment_line_items') }} pl
WHERE pl.payment_line_item_type <> 'refund'
  AND pl.invoice_amount <> 0
  AND pl.transaction_amount <> 0
