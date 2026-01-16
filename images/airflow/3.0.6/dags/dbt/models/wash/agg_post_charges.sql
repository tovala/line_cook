
SELECT 
  payment_id
  , COALESCE(SUM(CASE WHEN post_charge_type = 'charge' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_gross_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'gift_card' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_gift_card_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'meal_credit' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_meal_credit_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'meal_cash' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_meal_cash_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'discount' OR post_charge_type = 'marketing_cash'
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_discount_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'shipping' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_shipping_amount
  , COALESCE(SUM(CASE WHEN post_charge_type = 'tax' 
                      THEN post_charge_amount 
                      ELSE 0 
                 END), 0) AS post_sale_tax_amount
  , COALESCE(SUM(COALESCE(post_charge_amount,0)), 0) AS post_sale_charge_amount
  , COALESCE(MIN(CASE WHEN post_charge_type = 'charge' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'gift_card' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'discount' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'meal_credit' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'meal_cash' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'shipping' THEN post_charge_time END),
             MIN(CASE WHEN post_charge_type = 'tax' THEN post_charge_time END)) AS post_sale_charge_time
  , LISTAGG(post_charge_type || ' : ' || post_charge_notes, ' | ') AS post_sale_notes
FROM {{ ref('post_charges') }}
GROUP BY 1