
-- TO DO: use this table if we end up with other excluded coupons (for ex: influencer ovens) 
SELECT 
  cc.userid AS customer_id
  , pli.payment_id
  , p.created AS coupon_use_time
  , cc.coupon_code
FROM {{ table_reference('coupon_codes_applied') }} cc
INNER JOIN {{ table_reference('payment_line_item') }} pli
ON cc.id = pli.coupon_code_applied_id
INNER JOIN {{ table_reference('payment') }} p 
ON pli.payment_id = p.id
-- Currently, only applying this to San Marco ovens
WHERE cc.coupon_code ILIKE 'smp%'