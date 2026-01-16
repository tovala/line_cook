
SELECT
  transaction_id
  , discount_id
  , redemption_cd
  , redeemer_customer_id AS customer_id
  , generated_time
  , transaction_amount
  , balance_remaining_amount
  , transaction_type
  , FIRST_VALUE(transaction_type) OVER (PARTITION BY discount_id ORDER BY transaction_number) AS first_transaction_type
  , credit_type
  , payment_id
  , transaction_time
  , transaction_number
  , is_depleted
  , is_first_transaction
  , is_most_recent_transaction
  , stripe_charge_id
  , is_customer_service_credit
  , cs_reason
FROM {{ ref('gc_and_md_summary') }}
WHERE is_marketing_discount