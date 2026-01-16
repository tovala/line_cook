
SELECT 
  payment_id
  , COALESCE(SUM(CASE WHEN charge_type = 'sku' OR charge_type = 'sku_menuproduct'
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS gross_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'gift_card' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS gift_card_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'meal_credit' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS meal_credit_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'meal_cash' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS meal_cash_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'discount' OR charge_type = 'marketing_cash'
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS discount_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'shipping' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS shipping_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'tax' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS tax_amount
  , COALESCE(SUM(CASE WHEN charge_type = 'charge' 
                      THEN charge_amount 
                      ELSE 0 
                 END), 0) AS charge_amount
  , COALESCE(MIN(CASE WHEN charge_type = 'charge' THEN charge_time END),
             MIN(CASE WHEN charge_type <> 'charge' THEN charge_time END)) AS charge_time
FROM {{ ref('charges') }}
GROUP BY 1