
WITH transactions_rows AS (
 SELECT
   transaction_id
   , discount_id
   , transaction_amount
   , transaction_type
   , payment_id
   , transaction_time
   , stripe_charge_id
   , is_customer_service_credit
   , cs_reason
   , ROW_NUMBER() OVER (PARTITION BY discount_id
                        ORDER BY transaction_time) AS transaction_number
  FROM {{ ref('gc_and_md_transactions') }}
)
SELECT 
  gt.transaction_id
  , gt.discount_id
  , gc.redemption_cd
  , gc.redeemer_customer_id
  , gc.buyer_customer_id
  , gc.buyer_email
  , gc.generated_time AS generated_time
  , gc.is_marketing_discount
  , gc.credit_type
  , gt.transaction_amount
  -- Running total of transactions as of the transaction this row corresponds to being complete
  , SUM(gt.transaction_amount) OVER (PARTITION BY gt.discount_id
                                     ORDER BY gt.transaction_number 
                                     ROWS UNBOUNDED PRECEDING) AS balance_remaining_amount
  , gt.transaction_type
  , gt.payment_id
  , gt.transaction_time
  , gt.transaction_number
  , (balance_remaining_amount <= 0) AS is_depleted 
  , (gt.transaction_number = 1) AS is_first_transaction 
  , (gt.transaction_number = MAX(gt.transaction_number) OVER (PARTITION BY gt.discount_id)) AS is_most_recent_transaction
  , gt.stripe_charge_id
  , gt.is_customer_service_credit
  , gt.cs_reason
FROM transactions_rows gt
INNER JOIN {{ ref('gc_and_md') }} gc
ON gt.discount_id = gc.discount_id