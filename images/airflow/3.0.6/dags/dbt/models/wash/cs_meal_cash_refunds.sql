SELECT 
  payment_id 
  , customer_id
  , cs_ticket_id
  , cs_reason
  , notes 
  , transaction_time
  , ABS(meal_cash_amount) AS line_credit_amount
  , SUM(line_credit_amount) OVER (PARTITION BY payment_id, customer_id) AS mc_total_credit_amount 
  , cs_affected_mealids
  , action_category
FROM {{ ref('meal_cash') }}
WHERE meal_cash_amount > 0
  AND voided IS NULL
  AND action_category <> 'debit'
  -- Exclude meal-credits that were artificially transformed into meal cash for the sake of sanity. Including these confounds the data (makes it look like a bunch of meal cash went out in Feb 2021, which is just when we sunset meal credits and converted outstanding credits into meal cash).
  AND notes <> 'DEA-393 mealcredit-migration'
