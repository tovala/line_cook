
WITH chargeback_to_payment AS (
  SELECT DISTINCT
    sd.id AS chargeback_id
    , sd.charge AS charge_id
    , COALESCE(p.payment_id, pli.payment_id) AS payment_id
    , sd.created::timestamp_ntz AS chargeback_submission_time
    , {{ cents_to_usd('sd.amount') }} AS chargeback_amount 
    , sd.reason AS chargeback_reason
    , sd.stripe_status AS chargeback_status   
  FROM {{ table_reference('stripe_disputes') }} sd
  LEFT JOIN {{ ref('payments') }} p
    ON sd.charge = p.stripe_charge_id
  LEFT JOIN {{ ref('payment_line_items') }} pli 
    ON sd.charge = pli.stripe_charge_id)
SELECT
  cb.chargeback_id
  , cb.charge_id
  , cb.payment_id
  , CASE WHEN mo.meal_order_id IS NOT NULL 
         THEN 'meal_order'
         WHEN oo.oven_order_id IS NOT NULL 
         THEN 'oven_order'
         WHEN gc.payment_id IS NOT NULL
         THEN 'gift_card_purchase'
    END AS order_type
  , mo.meal_order_id
  , mo.term_id AS meal_order_term_id 
  , oo.oven_order_id
  , gc.transaction_id AS gift_card_transaction_id
  , COALESCE(mo.order_time, oo.order_time, gc.transaction_time)::timestamp_ntz AS order_time
  , cb.chargeback_submission_time::timestamp_ntz AS chargeback_submission_time
  , cb.chargeback_amount
  , cb.chargeback_reason
  , cb.chargeback_status  
FROM chargeback_to_payment cb 
LEFT JOIN {{ ref('meal_orders') }} mo 
    ON cb.payment_id = mo.payment_id
LEFT JOIN {{ ref('oven_orders') }} oo 
    ON cb.payment_id = oo.payment_id
LEFT JOIN {{ ref('gift_cards_summary') }} gc 
    ON cb.payment_id = gc.payment_id
    AND gc.transaction_type = 'card_purchase'
WHERE cb.payment_id IS NOT NULL
