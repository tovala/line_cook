
SELECT 
  cc.code AS coupon_cd 
  , cc.redeem_by AS expiration_time
  , cc.created AS coupon_created_time
  , cc.userid AS customer_id 
  , cc.max_redemptions
  , cc.userid IS NOT NULL AS is_customer_specific
  , cc.dollar_amount AS dollar_discount_amount
  , cc.dollar_percent AS dollar_discount_percent
  , cc.description AS coupon_description
  , COALESCE(coupon_cd ILIKE 'amz-tvla%', FALSE) AS is_amazon_purchase_coupon
FROM {{ table_reference('coupon_codes') }} cc
WHERE customer_id IN (SELECT customer_id FROM {{ ref('customers') }})
  OR customer_id IS NULL
