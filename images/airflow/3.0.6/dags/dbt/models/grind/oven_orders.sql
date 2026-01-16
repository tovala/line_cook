
SELECT
  pp.id AS oven_order_id
  , pp.userid AS customer_id 
  , pp.coupon_code AS coupon_cd -- case specific
  , UPPER(pp.referral_code) AS referral_cd
  , pp.affirm_charge_id AS affirm_charge_id
  , pp.affirm_charge_id IS NOT NULL AS is_affirm_order
  , pp.payment_id AS payment_id 
  , pp.created AS order_time 
  , pp.product_id
  , CASE WHEN oof.orderstatus IN ('shipped', 'delivered', 'paid') 
         THEN 'complete'  
         WHEN oof.orderstatus IN ('refunded', 'canceled')
         THEN oof.orderstatus
    END AS status
  , CASE WHEN is_affirm_order 
         THEN aoi.unit_price_amount
         ELSE ac.gross_amount 
    END AS oven_sale_price
  , CASE WHEN is_affirm_order 
         THEN oven_sale_price + aoi.discount_amount
         ELSE oven_sale_price + COALESCE(ac.discount_amount, 0) + COALESCE(apc.post_sale_discount_amount, 0) 
    END AS oven_purchase_price
  , CASE WHEN is_affirm_order 
         THEN ao.total_charge_amount
         ELSE COALESCE(ac.charge_amount, 0) + COALESCE(apc.post_sale_gross_amount, 0)
    END AS oven_charge_amount
  , p.charge_sku AS oven_order_sku 
  , oof.sku AS original_oven_order_sku
  -- TODO: It would be nice to be able to identify the ovens that are $49 or $99 and call them out as such
  , p.oven_generation
  , CASE WHEN p.oven_generation IN ('launch', 'rental')
         THEN INITCAP(p.oven_generation) || ' Oven'
         WHEN p.oven_generation IN ('gen_2', 'gen_1', 'airvala')
         -- Follows the format: (Gen1|Gen2) $X ?(Gift Bundle|Influencer) ?(X Week Commitment|X Meal Commitment) Oven ?(with $X Shipping)
         THEN INITCAP(REPLACE(p.oven_generation, '_', '')) 
              || ' $' || ROUND(p.base_price, 2)
              || COALESCE(' ' || INITCAP(REPLACE(p.special_category, '_', ' ')), '') 
              || IFF(p.is_commitment_product, ' ' || INITCAP(REPLACE(p.commitment_type, '_', ' ')) || ' Commitment Oven', ' Oven')
              || IFF(p.shipping_charge > 0, ' with $' || ROUND(p.shipping_charge, 2) || ' Shipping', '')
    END AS oven_order_sku_formatted
  , oven_order_sku ILIKE 'rental%' AS is_rental_oven
  , p.is_commitment_product OR poof.is_commitment_product AS is_commitment_purchase
  , poof.is_commitment_product AND NOT p.is_commitment_product AS is_commitment_forgiven 
  , COALESCE(poof.commitment_week_count, p.commitment_week_count) AS commitment_week_count
  , p.activate_subscription AS subscription_activated_on_purchase
  , (COALESCE(cc.dollar_discount_percent, 0) = 100 OR p.total_price = 0) 
     -- San Marco ovens shouldn't be considered free
     AND NOT pp.coupon_code ILIKE 'smp%' AS is_free_oven
  , oof.id AS oven_order_fulfillment_id
  , pp.products_shipment_id AS products_shipment_id
  , oof.stripe_orderid AS stripe_order_id
  , MIN(te.term_id) AS first_orderable_term_id
  , ROW_NUMBER() OVER (PARTITION BY pp.userid ORDER BY pp.created) AS nth_order
  , CASE WHEN status='canceled' 
         THEN NULL  
         ELSE COUNT_IF(status!='canceled') OVER (PARTITION BY pp.userid ORDER BY pp.created ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    END AS nth_fulfilled_order 
  , nth_fulfilled_order = 1 AS is_first_oven_purchase
FROM {{ table_reference('products_purchase') }} pp 
INNER JOIN {{ ref('products') }} p 
  ON pp.product_id = p.product_id AND p.product_category = 'd2c_oven'
INNER JOIN {{ref ('customers') }} c -- Filter out testvala users
  ON pp.userid = c.customer_id
LEFT JOIN {{ table_reference('ovenorderfulfillment') }} oof 
  ON pp.ovenorderfulfillment_id = oof.id 
LEFT JOIN {{ref ('coupons') }} cc 
  ON cc.coupon_cd = pp.coupon_code 
LEFT JOIN {{ ref('terms') }} te 
  ON pp.created < te.order_by_time 
  AND NOT te.is_company_holiday 
  AND (te.is_past_order_by OR te.is_live) 
LEFT JOIN {{ ref('agg_charges') }} ac
  ON pp.payment_id = ac.payment_id
LEFT JOIN {{ ref('agg_post_charges') }} apc
  ON pp.payment_id = apc.payment_id
LEFT JOIN {{ ref('affirm_orders')}} ao
  ON pp.affirm_charge_id = ao.internal_affirm_charge_id
LEFT JOIN {{ ref('affirm_order_items') }} aoi
  ON ao.charge_id = aoi.charge_id
LEFT JOIN {{ ref('products') }} poof 
  ON oof.sku = poof.charge_sku
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
