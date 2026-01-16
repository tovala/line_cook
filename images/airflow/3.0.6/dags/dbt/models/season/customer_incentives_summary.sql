
WITH gift_cards AS (
  SELECT 
    redeemer_customer_id
    , SUM(transaction_amount) AS transaction_amount
    , SUM(CASE WHEN transaction_amount > 0 THEN transaction_amount ELSE 0 END) AS total_amount
    , COUNT(DISTINCT discount_id) AS total_count
  FROM {{ ref('gift_cards_summary') }}
  GROUP BY 1
) , marketing_discounts AS (
  SELECT 
    customer_id
    , SUM(transaction_amount) AS transaction_amount
    , SUM(CASE WHEN transaction_amount > 0 THEN transaction_amount ELSE 0 END) AS total_amount
    , COUNT(DISTINCT discount_id) AS total_count
  FROM {{ ref('marketing_discounts') }}
  GROUP BY 1
) , meal_cash AS (
  SELECT 
    customer_id
    , SUM(meal_cash_amount) AS meal_cash_amount
    , SUM(CASE WHEN meal_cash_amount > 0 THEN meal_cash_amount ELSE 0 END) AS total_amount
    , COUNT(DISTINCT CASE WHEN meal_cash_amount > 0 THEN transaction_id END) AS total_count
  FROM {{ ref('meal_cash') }}
  WHERE voided IS NULL
  GROUP BY 1
)
SELECT 
  c.customer_id
  , COALESCE(gcs.total_count,0) AS lifetime_gift_card_count
  , COALESCE(gcs.total_amount,0) AS lifetime_gift_card_amount
  , COALESCE(gcs.transaction_amount,0) AS current_gift_card_balance
  , COALESCE(md.total_count,0) AS lifetime_marketing_discount_count
  , COALESCE(md.total_amount,0) AS lifetime_marketing_discount_amount
  , COALESCE(md.transaction_amount,0) AS current_marketing_discount_balance
  , COALESCE(mc.total_count,0) AS lifetime_meal_cash_credit_count
  , COALESCE(mc.total_amount,0) AS lifetime_meal_cash_amount
  , COALESCE(mc.meal_cash_amount,0) AS current_meal_cash_balance
  , lifetime_gift_card_count + lifetime_marketing_discount_count + lifetime_meal_cash_credit_count AS lifetime_incentive_count
  , lifetime_gift_card_amount + lifetime_marketing_discount_amount + lifetime_meal_cash_amount AS lifetime_incentive_amount
  , current_gift_card_balance + current_marketing_discount_balance + current_meal_cash_balance AS current_incentive_balance
FROM {{ ref('customers') }} c
LEFT JOIN gift_cards gcs
  ON c.customer_id = gcs.redeemer_customer_id
LEFT JOIN marketing_discounts md
  ON c.customer_id = md.customer_id
LEFT JOIN meal_cash mc
  ON c.customer_id = mc.customer_id