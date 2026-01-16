{{
  config(
    tags=["retool"]
  ) 
}}

-- Used by: Demand Planning/Cohort Model Fulfilled Cycle Split by Cohort 
-- Used by: Demand Planning/Cohort Model Fulfilled Orders by Size, Cohort, and Term

SELECT 
  {{ facility_network('uss.facility_network') }} AS facility_network
  , cts.term_id
  , cts.cohort
  , cts.customer_id
  , cf.first_order_size
  , mo.order_size
  , mo.cycle
  , COUNT(DISTINCT mo.meal_order_id) AS fulfilled_order_count
  , mo.order_size * fulfilled_order_count AS fulfilled_meal_count
FROM {{ ref('customer_term_summary') }} cts
LEFT JOIN {{ ref('customer_facts') }} cf
  ON cts.customer_id = cf.customer_id
LEFT JOIN {{ ref('meal_orders') }} mo
  ON cts.meal_order_id = mo.meal_order_id
LEFT JOIN {{ ref('upcoming_subterm_shipping') }} uss
  ON cts.zip_cd = uss.zip_cd
  AND cts.cycle = uss.cycle
  AND uss.term_id = {{ live_term(availability_check=True) }}
WHERE cts.cohort >= 18
  AND cts.is_after_cohort_term
  AND cts.is_fulfilled
  AND NOT cts.is_holiday_term
  AND NOT cts.is_internal_account
  AND NOT mo.is_trial_order
GROUP BY 1,2,3,4,5,6,7
