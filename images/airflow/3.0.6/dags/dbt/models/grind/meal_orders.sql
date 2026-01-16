
SELECT 
    uto.id AS meal_order_id
    , uto.userid AS customer_id 
    , uto.termid AS term_id
    , st.subterm_id
    , uto.menu_product_order_id AS add_on_order_id
    , CASE WHEN orf.orderstatus IN ('canceled', 'refunded')
           THEN orf.orderstatus
           WHEN uto.status IN ('payment_error', 'payment_errors', 'error', 'payment_authorized_error')
           THEN 'payment_error'
           WHEN uto.status = 'on_break'
           THEN 'skipped'
           ELSE uto.status 
      END AS status
    , uto.status = 'on_break' AS is_break_skip
    , st.cycle 
    , st.facility_network
    -- Basically, meal_order.status IN ('complete', 'refunded')
    , COALESCE(orf.orderstatus = 'refunded', FALSE) 
        OR (COALESCE(orf.orderstatus <> 'canceled', FALSE) AND uto.status = 'complete') AS is_fulfilled
    , uto.subscriptiontypes_id AS subscription_type
    , uto.payment_id AS payment_id 
    , uto.orderfulfillment_id AS meal_order_fulfillment_id 
    , uto.products_purchase_id AS products_purchase_id 
    , uto.notes AS order_notes 
    , uto.meal_count AS order_size
    , uto.created AS order_time
    , uto.updated AS order_updated_time
    , COALESCE(orf.reason = 'canceled-by-tovala', FALSE) AS was_canceled_by_tovala
    , COALESCE(p.charge_sku, '') = 'trialvala_v1' OR (uto.payment_id IS NULL AND c.is_influencer) AS is_trial_order
    , COALESCE(p.charge_sku = 'gma_oven_bundle_v1', FALSE) OR uto.subscriptiontypes_id = 'gma_voucher_v1' AS is_gma_order
    , COALESCE(p.charge_sku NOT IN ('trialvala_v1', 'gma_oven_bundle_v1'), FALSE) AS is_commitment_order
    -- This is not populated after term 67 but it is useful for tying meal orders to stripe payments during that time
    , orf.stripe_orderid AS stripe_order_id
    , {{ parse_zendesk_ticket_id('orf.zd_ticket_number') }} AS cs_ticket_id
    , orf.reason AS cs_reason
    -- Ensuring that an empty array here yields a NULL
    , NULLIF(orf.meals, ARRAY_CONSTRUCT()) AS cs_affected_meal_ids
    , orf.refunded_date AS cs_refund_time
    , sh.stripe_fingerprint AS stripe_fingerprint
    -- TODO: Add the following to mart
       -- is_commitment_order - products purchase id being null is not sufficient
FROM {{ table_reference('user_term_order') }} uto 
LEFT JOIN {{ ref('subterms') }} st 
  ON uto.subterm_id = st.subterm_id 
LEFT JOIN {{ table_reference('orderfulfillment') }} orf 
  ON uto.orderfulfillment_id = orf.id
LEFT JOIN {{ table_reference('products_purchase') }} pp 
  ON uto.products_purchase_id = pp.id
LEFT JOIN {{ ref('products') }} p
  ON pp.product_id = p.product_id
LEFT JOIN {{ ref('stripe_fingerprint_history') }} sh 
    ON uto.userid = sh.customer_id
    AND uto.created >= sh.status_start_time
    AND uto.created < COALESCE(sh.status_end_time, '9999-12-31')
-- Excludes customers based on logic in leads table as well as customers who have never had a completed order
INNER JOIN {{ ref('customers') }} c 
  ON uto.userid = c.customer_id
