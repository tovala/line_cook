          
SELECT 
  p.payment_id
  , COALESCE(mo.meal_order_id, oo.oven_order_id, mem.rental_oven_payment_id) AS order_id
  , p.customer_id -- PREVIOUSLY: user_id
  -- Stripe-specific identifier for a specific card/bank account for fraud-detection purposes 
  , mo.stripe_fingerprint AS stripe_fingerprint_id 
  , COALESCE(oo.oven_order_fulfillment_id, mo.meal_order_fulfillment_id, mem.rental_oven_payment_id) AS shipment_id
  , COALESCE(mo.stripe_order_id, oo.stripe_order_id) AS stripe_order_id
  , psi.stripe_charge_ids
  -- TermID is not applicable to oven orders or shipments
  , COALESCE(mo.term_id, mem.term_id) AS term_id
  , st.subterm_id 
  , CASE WHEN ecp.coupon_code ILIKE 'smp%'
         -- TO DO: come up with a category that is better than this
         THEN 'canceled'
         WHEN COALESCE(mship.status, oship.order_status) IS NOT NULL 
         THEN COALESCE(mship.status, oship.order_status)
         WHEN rental_oven_payment_id IS NOT NULL
         THEN mem.order_status
         WHEN mo.meal_order_id IS NOT NULL
         THEN mo.status
         WHEN oo.oven_order_id IS NOT NULL
         THEN oo.status
    END AS fulfillment_status
  , CASE WHEN fulfillment_status = 'canceled' OR ecp.payment_id IS NOT NULL
         THEN 'excluded'
         WHEN COALESCE(mship.product_type, oship.product_type) IS NOT NULL 
         THEN COALESCE(mship.product_type, oship.product_type)
         WHEN rental_oven_payment_id IS NOT NULL
         THEN 'rental_oven' -- PREVIOUSLY: "membership"
         WHEN mo.meal_order_id IS NOT NULL
         THEN 'meal'
         WHEN oo.oven_order_id IS NOT NULL
         THEN 'oven'
    END AS category
  , CASE WHEN oo.oven_order_id IS NOT NULL 
         THEN oo.oven_order_sku
         WHEN mo.meal_order_id IS NOT NULL 
         THEN CASE WHEN mo.subscription_type ILIKE 'three_weekly%'
                   THEN 'food_three_weekly'
                   WHEN mo.subscription_type ILIKE 'three_by_two_weekly%'
                   THEN 'food_three_by_two_weekly'
                   ELSE mo.subscription_type 
              END 
         WHEN mem.payment_id IS NOT NULL 
         THEN mem.membership_type_id 
    END AS sku 
  , COALESCE(mship.destination_state, oship.destination_state, a.state) AS destination_state -- PREVIOUSLY: state_abbr
  , COALESCE(mship.destination_zip_cd, oship.destination_zip_cd, a.zip_cd) AS destination_zip_cd
  , COALESCE(mship.destination_city, oship.destination_city, a.city) AS destination_city
  , COALESCE(c.gross_amount, 0) AS gross_amount
  , COALESCE(c.gift_card_amount, 0) AS gift_card_amount
  , COALESCE(c.meal_credit_amount, 0) AS meal_credit_amount
  , COALESCE(c.meal_cash_amount, 0) AS meal_cash_amount
  , COALESCE(c.discount_amount, 0) AS discount_amount
  , COALESCE(c.shipping_amount, 0) AS shipping_amount
  , COALESCE(c.tax_amount, 0) AS tax_amount
  , COALESCE(c.charge_amount, 0) AS charge_amount
  , COALESCE(c.charge_time, p.payment_time) AS charge_time -- in the very rare case there isn't a charge associated with payment, use payment time
  , COALESCE(pc.post_sale_gross_amount, 0) AS post_sale_gross_amount
  , COALESCE(pc.post_sale_gift_card_amount, 0) AS post_sale_gift_card_amount
  , COALESCE(pc.post_sale_meal_credit_amount, 0) AS post_sale_meal_credit_amount
  , COALESCE(pc.post_sale_meal_cash_amount, 0) AS post_sale_meal_cash_amount
  , COALESCE(pc.post_sale_discount_amount, 0) AS post_sale_discount_amount
  , COALESCE(pc.post_sale_shipping_amount, 0) AS post_sale_shipping_amount
  , COALESCE(pc.post_sale_tax_amount, 0) AS post_sale_tax_amount
  , COALESCE(pc.post_sale_charge_amount, 0) AS post_sale_charge_amount
  , pc.post_sale_charge_time
  , pc.post_sale_notes
  , COALESCE(ar.gross_refund_amount, 0) AS gross_refund_amount
  , COALESCE(ar.meal_credit_refund_amount, 0) AS meal_credit_refund_amount
  , COALESCE(ar.meal_cash_refund_amount, 0) AS meal_cash_refund_amount
  , COALESCE(ar.gift_card_refund_amount, 0) AS gift_card_refund_amount
  , COALESCE(ar.tax_refund_amount, 0) AS tax_refund_amount
  , COALESCE(ar.refund_amount, 0) AS refund_amount
  -- This is used to calculate AOV since that is shipping+sku/number of orders - currently only for meal orders
  , CASE WHEN category = 'meal'
         THEN gross_amount + shipping_amount  
    END AS order_value 
  , CASE WHEN mo.is_fulfilled AND NOT mo.is_trial_order AND NOT cust.is_internal_account
         THEN order_value 
    END AS meal_arr
  , ar.refund_time
  , ar.refund_notes
  , p.payment_time
  -- This should be shipment time for products that are shippable (i.e. meals/ovens) and payment time for anything else (i.e. rental payments)
  -- Should only be null if order is not shipped 
  , CASE WHEN category = 'meal'
         THEN st.subterm_ship_time
         WHEN category = 'oven'
         THEN oship.shipment_time 
         WHEN category = 'rental_oven'
         THEN p.payment_time 
    END AS revenue_recognized_time 
  -- TO DO: remove this when we start using revenue recognized time correctly
  , COALESCE(DATE_PART('month', t.start_date), 
             DATE_PART('month', CONVERT_TIMEZONE('America/Chicago', COALESCE(c.charge_time, p.payment_time))::DATE)) AS term_month
    -- Charge occurs during the previous month:
         -- For meals: based on term start date 
         -- For ovens: based on charge time
  , (CASE WHEN category = 'meal'
          THEN DATE_TRUNC('month', t.start_date)
          ELSE DATE_TRUNC('month', CONVERT_TIMEZONE('America/Chicago', COALESCE(c.charge_time, p.payment_time))::TIMESTAMP_NTZ)
     END = DATEADD('month', -1, DATE_TRUNC('month', {{ current_timestamp_chicago() }})::TIMESTAMPNTZ))
     -- Or there is a post charge during the previous month:
     OR
     (COALESCE(DATE_TRUNC('month', CONVERT_TIMEZONE('America/Chicago', pc.post_sale_charge_time)::TIMESTAMP_NTZ), '0000-01-01') 
      = DATEADD('month', -1, DATE_TRUNC('month', {{ current_timestamp_chicago() }})::TIMESTAMPNTZ))
     -- Or there is a refund during the previous month
     OR
     (COALESCE(DATE_TRUNC('month', CONVERT_TIMEZONE('America/Chicago', ar.refund_time)::TIMESTAMP_NTZ), '0000-01-01') 
      = DATEADD('month', -1, DATE_TRUNC('month', {{ current_timestamp_chicago() }})::TIMESTAMPNTZ))
     -- Or there is a meal cash refund during the previous month
     OR
     (COALESCE(DATE_TRUNC('month', CONVERT_TIMEZONE('America/Chicago', mcr.refund_time)::TIMESTAMP_NTZ), '0000-01-01')  
      = DATEADD('month', -1, DATE_TRUNC('month', {{ current_timestamp_chicago() }})::TIMESTAMPNTZ))
    AS is_in_previous_month  
FROM {{ ref('payments') }} p
LEFT JOIN {{ ref('meal_orders') }} mo ON p.payment_id = mo.payment_id
LEFT JOIN {{ ref('oven_orders') }} oo ON p.payment_id = oo.payment_id
LEFT JOIN {{ ref('rental_oven_payments') }} mem ON p.payment_id = mem.payment_id
LEFT JOIN {{ ref('meal_shipments') }} mship ON mo.meal_order_id = mship.meal_order_id
LEFT JOIN {{ ref('oven_shipments') }} oship ON oo.oven_order_id = oship.oven_order_id
LEFT JOIN {{ ref('addresses') }} a ON (a.customer_id = p.customer_id AND mem.rental_oven_payment_id IS NOT NULL)
LEFT JOIN {{ ref('agg_charges') }} c ON p.payment_id = c.payment_id
LEFT JOIN {{ ref('agg_post_charges') }} pc ON p.payment_id = pc.payment_id
LEFT JOIN {{ ref('agg_refunds') }} ar ON p.payment_id = ar.payment_id
LEFT JOIN {{ ref('payment_to_stripe_id') }} psi ON p.payment_id = psi.payment_id
LEFT JOIN {{ ref('excluded_coupon_payments') }} ecp ON p.payment_id = ecp.payment_id
LEFT JOIN {{ ref('terms') }} t ON t.term_id = mo.term_id
LEFT JOIN {{ ref('customers') }} cust ON mo.customer_id = cust.customer_id
LEFT JOIN {{ ref('subterms') }} st ON mo.subterm_id = st.subterm_id
LEFT JOIN {{ ref('meal_cash_refunds') }} mcr ON p.payment_id = mcr.payment_id
WHERE p.payment_status IN ('paid', 'refunded_partial', 'refunded', 'refunded_mealcash')
  AND COALESCE(mo.meal_order_id, oo.oven_order_id, mem.rental_oven_payment_id) IS NOT NULL
