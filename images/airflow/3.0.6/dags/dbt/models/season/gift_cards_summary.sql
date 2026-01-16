
SELECT
  transaction_id
  , discount_id
  , redemption_cd
  , redeemer_customer_id
  , buyer_customer_id
  , buyer_email
  , generated_time
  , transaction_amount
  , balance_remaining_amount
  , transaction_type
  , payment_id
  , transaction_time
  , transaction_number
  , is_depleted
  , is_first_transaction
  , is_most_recent_transaction
  , stripe_charge_id
  , cs_reason
FROM {{ ref('gc_and_md_summary') }}
WHERE NOT is_marketing_discount