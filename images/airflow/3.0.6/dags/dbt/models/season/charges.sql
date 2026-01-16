
SELECT 
  pl.payment_line_item_id AS charge_id
  , pl.payment_id
  , pl.payment_line_item_type AS charge_type
  , CASE WHEN pl.payment_line_item_type = 'charge'
         THEN pl.transaction_amount
         ELSE pl.invoice_amount
    END AS charge_amount 
  , pl.payment_line_item_notes AS charge_notes
  , pl.payment_time AS charge_time
FROM {{ ref('payment_line_items') }} pl
WHERE (pl.payment_line_item_type = 'charge' AND pl.invoice_amount = 0 AND pl.transaction_amount <> 0)
  OR (pl.payment_line_item_type IN ('meal_credit', 'meal_cash', 'discount', 'marketing_cash', 'gift_card') 
      AND pl.invoice_amount <> 0 AND pl.transaction_amount = 0)
  OR (pl.payment_line_item_type IN ('sku', 'sku_menuproduct', 'tax', 'shipping'))
