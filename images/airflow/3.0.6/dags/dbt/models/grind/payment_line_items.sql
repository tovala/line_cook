
SELECT
  pli.id AS payment_line_item_id 
  , pli.payment_id 
  , {{ cents_to_usd('pli.invoice_amount_cents') }} AS invoice_amount
  , {{ cents_to_usd('pli.transaction_amount_cents') }} AS transaction_amount
  , CASE WHEN pli.type = 'giftcard' 
         THEN 'gift_card' 
         ELSE pli.type 
    END AS payment_line_item_type
  , pli.notes AS payment_line_item_notes
  , pli.coupon_code AS coupon_cd
  , UPPER(pli.referral_code) AS referral_cd
  , pli.coupon_code_applied_id AS coupon_redemption_id -- todo: figure out what this is and create a table with it as the primary key
  , pli.created AS payment_time 
  , pli.stripe_charge_id 
FROM {{ table_reference('payment_line_item') }} pli
INNER JOIN {{ ref('payments') }} p ON pli.payment_id = p.payment_id
WHERE pli.type IS NOT NULL
