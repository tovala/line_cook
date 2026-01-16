{{
  config(
    tags=["retool"]
  ) 
}}

-- Used by: Demand Planning/RTT - Final Term Counts
-- Used by: Demand Planning/Upcoming Term Meal Order Count

SELECT
  cts.term_id
  --Note: CHICAGO is a catch all due to an error being thrown when cycle 2 zips historically are not cycle 2 in upcoming_subterm_shipping table eg) cust 438082 for term 246
  --To Do: Think of a better solution for this...
  , {{ facility_network('uss.facility_network') }} AS facility_network
  , COUNT(DISTINCT CASE WHEN cts.order_status <> 'payment_delinquent' AND cts.subscription_status = 'active' AND cts.is_after_cohort_term
                        THEN cts.customer_id 
                   END) AS active_users
  , COUNT(DISTINCT CASE WHEN cts.subscription_status = 'paused' AND cts.is_after_cohort_term
                        THEN cts.customer_id 
                   END) AS paused_users
  , COUNT(DISTINCT CASE WHEN cts.subscription_status = 'canceled' AND cts.is_after_cohort_term
             			      THEN cts.customer_id 
                   END) AS canceled_users
  , COUNT(DISTINCT CASE WHEN cts.order_status = 'payment_delinquent' AND cts.subscription_status = 'active' AND cts.is_after_cohort_term
                        THEN cts.customer_id 
                   END) AS account_delinquent
  , SUM(CASE WHEN mo.is_fulfilled 
             THEN mo.order_size 
        END) AS total_meals_shipped
  , COUNT(DISTINCT CASE WHEN mo.is_fulfilled 
                        THEN mo.customer_id 
                   END) AS term_meal_orders
  , COUNT(DISTINCT CASE WHEN cts.order_status = 'skipped' 
                        THEN cts.customer_id 
                   END) AS term_skips
  , COUNT(DISTINCT CASE WHEN cts.order_status = 'canceled' 
                        THEN cts.customer_id 
                   END) AS term_canceled_orders
  , COUNT(DISTINCT CASE WHEN cts.order_status = 'payment_error' 
                        THEN cts.customer_id 
                   END) AS term_payment_errors
  , SUM(CASE WHEN cts.order_status = 'payment_delinquent' 
             THEN 1 
        END) AS term_payment_delinquent
  , COUNT(DISTINCT CASE WHEN fss.is_forced_skip  AND fss.final_status = 'skipped' --There are cases where is_forced_skip = TRUE but final_status = 'unskipped'
                        THEN fss.customer_id 
                   END) AS forced_skips 
  , COUNT(DISTINCT CASE WHEN fss.is_break_skip  AND fss.final_status = 'skipped' 
                        THEN fss.customer_id 
                   END) AS break_skips
  , term_skips-forced_skips AS net_skips
  , AVG(CASE WHEN cts.order_status = 'complete' AND ms.status IN ('paid', 'shipped', 'refunded', 'delivered') 
             THEN mo.order_size 
        END) AS average_order_size
FROM {{ ref('customer_term_summary') }} cts
LEFT JOIN {{ ref('upcoming_subterm_shipping') }} uss
  ON cts.zip_cd = uss.zip_cd
  AND cts.cycle = uss.cycle
  AND uss.term_id = {{ live_term(availability_check=True) }}
LEFT JOIN {{ ref('meal_shipments') }} ms
  ON cts.meal_order_id = ms.meal_order_id
LEFT JOIN {{ ref('meal_orders') }} mo
  ON cts.meal_order_id = mo.meal_order_id
  AND NOT mo.is_gma_order
  AND NOT mo.is_trial_order
LEFT JOIN {{ ref('final_skip_statuses') }} fss
  ON cts.customer_id = fss.customer_id
  AND cts.term_id = fss.term_id
WHERE NOT cts.is_internal_account
GROUP BY 1,2
