-- Fraudsters brings together all revenue and refund information necessary to determine whether a customer may be committing fraud 
-- MVP: used in Sigma to flag customers who have outsized refund:revenue ratios (excludes 'oven fraud')
-- End goal: used to set a risk field for fraud (e.g. low, medium, high risk of fraud)

-- Pull meals affected by the refund for specific CS reasons 
WITH refunded_meals AS (
    SELECT 
        mc.customer_id
        , mc.payment_id
        , TO_NUMBER(TRIM(f.value)) AS meal_id  
    FROM {{ ref('meal_cash') }} mc,
    LATERAL FLATTEN(mc.cs_affected_mealids) f
    GROUP BY ALL 
)
-- Check if the customer cooked the refunded meals
, cooked_refunded_meals AS (
    SELECT 
        rm.customer_id
        , rm.payment_id 
        , COUNT(rm.meal_id) AS total_refunded_meals
        , COUNT_IF(ce.meal_sku_id IS NOT NULL) AS total_cooked_refunded_meals 
    FROM refunded_meals rm
    LEFT JOIN {{ ref('tovala_meal_cook_events') }} ce 
        ON rm.customer_id = ce.customer_id
        AND rm.meal_id = ce.meal_sku_id
    GROUP BY ALL  
)
SELECT DISTINCT 
    r.customer_id
    -- Payment ID should be not null for all paid orders 
    , r.payment_id 
    -- Stripe Fingerprint ID will be null for charges prior to Stripe implementing this field  
    , r.stripe_fingerprint_id  
    , r.term_id
    , (r.term_id - cts.first_order_term_id) AS weeks_since_first_order 
    , cts.term_subscription_refund_amount::FLOAT AS term_refund 
    , cts.order_size
    , cts.latest_meal_order_time
    , cts.latest_fulfilled_meal_order_time
    , r.meal_arr::FLOAT AS term_revenue 
    -- Revenue and refund totals for a customer's ENTIRE LIFETIME  
    , cts.running_total_subscription_refund_amount::FLOAT AS running_total_revenue_refunded 
    , cts.running_total_fulfilled_meal_revenue::FLOAT AS running_total_revenue 
    , cts.running_total_meal_refund_count
    , cts.running_total_refunded_terms
    -- Revenue and refund totals for THIS TERM 
    , COALESCE(cm.total_cooked_refunded_meals, 0) AS total_cooked_refunded_meals
    , COUNT_IF(cts.term_has_refund AND cm.total_cooked_refunded_meals>0) OVER (PARTITION BY r.customer_id) AS total_cooked_refunded_terms
    , ROUND(DIV0(term_refund, term_revenue), 2) AS pct_revenue_refunded 
    , ROUND(DIV0(COALESCE(cm.total_refunded_meals, 0), cts.order_size), 2) AS pct_meals_refunded
    , ROUND(DIV0(COALESCE(cm.total_cooked_refunded_meals, 0), total_refunded_meals), 2) AS pct_cooked_meals_refunded  
FROM {{ ref('revenue_aggregations') }} r
LEFT JOIN {{ ref('customer_term_summary') }} cts
    ON r.customer_id=cts.customer_id
    AND r.term_id=cts.term_id
LEFT JOIN refunded_meals rm
    ON r.customer_id=rm.customer_id
    AND r.payment_id=rm.payment_id 
LEFT JOIN cooked_refunded_meals cm 
    ON rm.customer_id = cm.customer_id
    AND r.payment_id=cm.payment_id 
WHERE NOT cts.is_employee AND NOT cts.is_internal_account

