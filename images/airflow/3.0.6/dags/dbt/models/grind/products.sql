
SELECT DISTINCT
  p.id AS product_id
  , p.charge_sku 
  , p.shipment_sku
  , p.description
  , p.commitment_meals AS commitment_meal_count
  , p.commitment_weeks AS commitment_week_count
  , {{ cents_to_usd('p.price_cents') }} AS total_price 
  , {{ cents_to_usd('p.gift_cents') }} AS gift_amount
  , COALESCE({{ cents_to_usd('p.shipping_cost_added') }}, 0) AS shipping_charge
  , total_price - shipping_charge AS base_price
  , COALESCE(SPLIT_PART(REGEXP_SUBSTR(p.charge_sku, '(msrp_\\d{2,3})'), '_', 2), base_price) AS listing_price
  , p.active AS is_active
  , p.activate_subscription
  , p.create_fulfillment
  , CASE WHEN p.charge_sku ILIKE 'amazon_%'
             OR p.charge_sku ILIKE 'qvc_%'
             OR p.charge_sku IN ('third_party_vendor_product', 'gma_oven_bundle_v1')
        THEN 'third_party_activation'
        WHEN p.charge_sku = 'sales_attribution_flow'
        THEN 'sales_attribution'
        WHEN p.charge_sku IN ('tovala_oven_qa', 'trial_test_charge', 'test_oven_1', 'tovala_oven_000')
             OR p.charge_sku IS NULL 
        THEN 'internal_use_case'
        WHEN p.charge_sku ILIKE 'apartmentvala%'
             OR p.charge_sku IN ('trialvala_v1', 'meals_only_flow')
        THEN 'meals_only'
        WHEN p.charge_sku ILIKE 'rental_program%'
             OR p.charge_sku ILIKE 'tovala_oven%'
             OR p.charge_sku IN ('gift_bundle_v1', 'gma_oven_bundle_v1', 'preorder_oven')
        THEN 'd2c_oven'
        ELSE 'uncategorized'
    END AS product_category
  , (commitment_meal_count > 0 OR commitment_week_count > 0) AS is_commitment_product
  , CASE WHEN p.charge_sku LIKE 'rental_program%' 
         THEN 'rental'
         WHEN p.charge_sku IN ('tovala_oven', 'preorder_oven')
         THEN 'launch'
         WHEN product_category = 'd2c_oven'
              AND p.charge_sku LIKE 'tovala_oven_air_%'
         THEN 'airvala'
	    WHEN product_category = 'd2c_oven'
              AND (p.charge_sku LIKE 'tovala_oven_gen2_%' 
                   OR p.charge_sku LIKE 'tovala_oven_steam%'
                   OR p.charge_sku = 'gift_bundle_v1')
         THEN 'gen_2'
         WHEN product_category = 'd2c_oven' 
              AND RLIKE(p.charge_sku, 'tovala_oven_(99|199|299|399).*')
         THEN 'gen_1'
    END AS oven_generation
  , CASE WHEN p.charge_sku = 'gift_bundle_v1'
         THEN 'gift_bundle'
         WHEN p.charge_sku = 'tovala_oven_99'
         THEN 'influencer'
    END AS special_category
  , COALESCE(IFF(commitment_meal_count > 0, commitment_meal_count || '_meal', NULL), 
             IFF(commitment_week_count > 0, commitment_week_count || '_week', NULL)) AS commitment_type
  , p.updated
FROM {{ table_reference('products') }} p
LEFT JOIN {{ table_reference('products_purchase') }} pp 
  ON p.id = pp.product_id 
WHERE (pp.product_id IS NOT NULL OR p.active) 
