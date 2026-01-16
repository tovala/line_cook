
-- TO DO: Oddities:
    -- Invoice amount shouldn't be part of the refund
    -- Refund amount should never be positive (or zero?)
    -- Potential payment_id dupes
    -- Verify that transaction_amount is correct (rather than invoice_amount)

SELECT 
  pl.payment_line_item_id AS refund_id
  , pl.payment_id
  , CASE WHEN pl.transaction_amount > 0 
         THEN -1* pl.transaction_amount
         ELSE pl.transaction_amount
    END AS refund_amount
  , pl.payment_line_item_notes AS refund_notes
  , pl.payment_time AS refund_time
FROM {{ ref('payment_line_items') }} pl
WHERE pl.payment_line_item_type = 'refund'
