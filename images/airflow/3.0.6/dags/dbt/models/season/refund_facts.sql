
--CS reasons from gift card payments are not included in this table. As of 12APR23, they make up about 7.5% of total records with non-null cs reasons
--To Do: Add gift card payments once the gift card data is more usable
--TO Do: Update the affected_meal_count logic anytime Tovala base price changes
WITH credits_refunds_and_cancellations AS (
  SELECT 
    CASE 
      WHEN mcr.payment_id IS NULL THEN 'refunded'
      ELSE 'credit_meal_cash'
    END AS refund_type
    , COALESCE(mo.customer_id, mcr.customer_id) AS customer_id
    , COALESCE(mo.payment_id, mcr.payment_id) AS payment_id
    , mo.term_id AS affected_term_id
    , mo.facility_network
    , COALESCE(mcr.cs_ticket_id, mo.cs_ticket_id) AS cs_ticket_id
    , COALESCE(mo.cs_reason, mcr.cs_reason) AS cs_reason_actual
    , CASE 
        WHEN 
          (cs_reason_actual IN ('food-shipping-issue|food-shipping-issue','food-shipping-issue', 'delayed-meals-melted-ice','missing-full-box','carrier-misdelivery','shipping-box-damaged','delayed-oven','meals-damaged-shipping-box-ok','outer-packaging-failure', 'ruptured-ice-pack','ruptured-ice-pack|ruptured-ice-pack','outer-packaging-failure|outer-packaging-failure','ops_issues_repeat_delays')
          OR (
            (cs_reason_actual IN ('canceled-by-tovala','refunded-by-tovala'))
            --Ops wants shipping delays to be rolled up with shipment_issues
            AND (
              (COALESCE(p.payment_notes, mcr.notes) ILIKE ('%Severe-Delays%'))
              OR ((COALESCE(p.payment_notes, mcr.notes) ILIKE ('%Fedex-Delays%')))
              )
            )
          )
        THEN 'shipping_error'
        -- Note: MSR-87 and MSR-1313 are one-off JIRA tickets related to a customer error 
        WHEN cs_reason_actual IN ('thought-account-was-inactive','forgot-to-skip|forgot-to-skip','customer-order-error|customer-order-error','thought-account-was-inactive|thought-account-was-inactive','customer-order-error','forgot-to-skip','MSR-87','MSR-1313','skip-pause-cancel-confusion','automatic-selection')
        THEN 'customer_error'
        WHEN cs_reason_actual IN ('food-quality|food-quality','food-quality','vacpack-seal-failure|vacpack-seal-failure','vacpack-seal-failure','single-vacpack-damaged','multiple-vacpack-damaged') 
        THEN 'food_quality_error'                        
        WHEN cs_reason_actual IN ('meal-component-error','packout-misc|packout-misc','meal-component-error|meal-component-error','packout-misc', 'wrong-meal-sent','wrong-meal-sent|wrong-meal-sent','missing-wrong-protein','missing-wrong-meal','missing-wrong-component') 
        THEN 'packout_error'
        WHEN cs_reason_actual IN ('meal-seal-failure','tray-seal-failure|tray-seal-failure','tray-seal-failure','packaging-failure-misc','melted-ice-pack','packaging-failure-misc|packaging-failure-misc','garnish-failure', 'melted-ice-pack|melted-ice-pack','on-time-meals-melted-ice','damaged-meal-component-not-vacpack','WEB-1700') 
        THEN 'packaging_quality_error'
        --Note: WEB-1700 is a one-off JIRA ticket related to an outer packaging failure
        WHEN cs_reason_actual IN ('marketing_cash_transition','referrer-cash','tovala_referrer_credit','customer_service','product_promotion','coupon_code_credit','marketing','other-non-meal-issues','retention','multiple-delivery-issues','less-than-one-meal-credit','add-promo-credit','referral-credit','partner-affiliate','marketplace-item-issue','oven-damage-credit','other-non-meal-issue')  
        THEN 'marketing'
        WHEN cs_reason_actual IN ('oven-return','oven-issues','cooking-issue','pending-exchange-return') 
        THEN 'oven_error'
        WHEN cs_reason_actual IN ('other-meal-issues|other-meal-issues','other-meal-issues','foreign-material','other-meal-order','customer-complaining','refunded-by-tovala-T305-C2-meal-swaps') 
        THEN 'other_meal_order'
        ELSE COALESCE(mo.cs_reason, mcr.cs_reason) 
      END AS cs_reason_category
    , CASE 
        WHEN refund_type = 'refunded' THEN COALESCE(mo.cs_refund_time, p.payment_time)
        ELSE COALESCE(mo.cs_refund_time, mcr.transaction_time)
      END AS cs_refund_time
    , mo.order_size
      --When gross amount is >$0, use gross amount to get average meal price
    , CASE WHEN ra.category = 'meal' AND ra.gross_amount > 0 THEN ra.gross_amount/mo.order_size 
          --If gross amount is $0, but charge amount is >$0, use the charge amount to get average meal price
          --As of 15MAY23, there is only 1 record where gross amount is $0
           WHEN ra.category = 'meal' AND ra.gross_amount = 0 AND ra.charge_amount > 0 THEN ra.charge_amount/mo.order_size
           --TO DO: Update this as base price changes
           --In case gross amounts AND charge amounts are $0, use a hard-coded base price. This should rarely be used.
           ELSE 12.99
      END AS average_meal_price
    , CASE 
        WHEN ra.charge_amount = 0 THEN ra.gross_amount 
        ELSE (ra.charge_amount+ra.post_sale_charge_amount) 
      END AS total_charge_amount
    , ABS(ra.refund_amount) AS total_refund_amount
    , mcr.line_credit_amount
    , CASE 
        WHEN mcr.payment_id IS NULL
        THEN mcr.line_credit_amount
        ELSE mcr.mc_total_credit_amount
      END AS total_credit_amount
    , CASE 
      WHEN refund_type = 'refunded' THEN COALESCE(ARRAY_SIZE(mo.cs_affected_meal_ids), FLOOR(ABS(total_refund_amount)/average_meal_price))
      WHEN refund_type = 'credit_meal_cash' THEN 
        CASE 
          WHEN (ARRAY_SIZE(mcr.cs_affected_mealids) = 0 AND mcr.action_category = 'debit') THEN NULL
          WHEN (ARRAY_SIZE(mcr.cs_affected_mealids) = 0 AND mcr.action_category <> 'debit') THEN FLOOR(ABS(total_credit_amount)/average_meal_price)
          ELSE ARRAY_SIZE(mcr.cs_affected_mealids)
        END
      END AS affected_meal_count
    , COALESCE(mo.status = 'canceled' AND p.payment_status <> 'paid', FALSE) AS was_canceled
    , mo.status AS meal_order_status
    , p.payment_status
    , COALESCE(p.payment_notes, mcr.notes) as notes
    , re.cs_remarks
    , CASE 
        WHEN refund_type = 'refunded' THEN 1
        ELSE 
          CASE 
            WHEN mcr.payment_id IS NULL 
            THEN ROW_NUMBER() OVER (PARTITION BY mcr.customer_id, mcr.transaction_time ORDER BY mcr.transaction_time ASC)
            ELSE ROW_NUMBER() OVER (PARTITION BY mcr.payment_id, mcr.customer_id, mo.term_id ORDER BY mcr.transaction_time ASC) 
          END
      END AS row_num
  FROM {{ ref('meal_orders') }} mo 
  INNER JOIN {{ ref('payments') }} p
    ON mo.payment_id = p.payment_id 
  LEFT JOIN {{ ref('revenue_aggregations') }} ra 
    ON mo.payment_id = ra.payment_id
  LEFT JOIN {{ ref('cs_meal_cash_refunds') }} mcr 
    ON mo.payment_id = mcr.payment_id  
  LEFT JOIN (
    SELECT DISTINCT * 
    FROM {{ ref('refund_events') }} 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cs_ticket_id ORDER BY refund_event_time DESC) = 1 -- if multiple comments, pull most recent
  ) re
    ON mo.cs_ticket_id::STRING = re.cs_ticket_id::STRING -- Note: We CAN join on meal_order_id, but that causes dupes sometimes
  -- Exclude marketing discounts
  WHERE COALESCE(mo.cs_reason, mcr.cs_reason) NOT IN ('marketing_cash_transition', 'referrer-cash', 'marketing') 
    -- Retain rows where cs_reason is NULL
    OR mo.cs_reason IS NULL
    OR mcr.cs_reason IS NULL
)
SELECT 
  refund_type
  , customer_id
  , payment_id
  , affected_term_id
  , facility_network
  , cs_ticket_id
  , cs_reason_actual AS cs_reason
  , cs_reason_category
  , cs_refund_time
  , order_size
  , LEAST(order_size, affected_meal_count) AS affected_meal_count
  , CASE 
      WHEN refund_type = 'refunded' THEN total_refund_amount
      ELSE total_credit_amount
    END AS amount
  , was_canceled
  , COALESCE(amount >= total_charge_amount, FALSE) AS was_entire_order_refunded
  , COALESCE(amount > total_charge_amount, FALSE) AS was_additional_refund_given
  , notes
  , cs_remarks
FROM credits_refunds_and_cancellations
WHERE row_num = 1
  -- Exclude rows from refunds (but not meal cash) where total_refund_amount is $0 or null
  AND NOT (refund_type = 'refunded' AND COALESCE(total_refund_amount, 0) = 0)
