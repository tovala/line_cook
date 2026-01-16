
-- This table will include a row for every term after a user appears in user_term_orders
-- This means that there can be rows PRIOR to a user's first completed order and it is also possible there are users who have never placed a completed order

WITH first_term AS (
  SELECT 
    customer_id
    , MIN(term_id) AS first_term
  FROM {{ ref('meal_orders') }}
  GROUP BY 1
),
refund_amounts AS (
  SELECT
    rf.customer_id
    , rf.affected_term_id
    , SUM(CASE WHEN rf.refund_type = 'refunded' THEN rf.amount END) AS term_subscription_refund_amount
    , SUM(rf.affected_meal_count) AS term_meal_refund_count
  FROM {{ ref('refund_facts') }} rf
  GROUP BY 1, 2
)
SELECT 
  -- This is unique on customer id and term id but concatenating them isn't (for ex: C-620 and 174 = C-6201 and 74)
  {{ hash_natural_key('ft.customer_id', 't.term_id', 't.order_by_time') }} AS customer_term_id
  , ft.customer_id 
  , t.term_id
  , cf.first_order_term_id
  , cf.cohort
  , cf.cohort_start_date
  , cf.latest_meal_order_time
  , cf.latest_fulfilled_meal_order_time
  , CASE WHEN t.term_id >= cf.first_order_term_id 
         THEN coh.week_with_holidays + cf.offset_with_holidays
    END AS cohort_week_with_holidays
  , CASE WHEN t.term_id >= cf.cohort 
         THEN coh.week_without_holidays + cf.offset_without_holidays
    END AS cohort_week_without_holidays
  , ft.first_term = t.term_id AS is_new_customer 
  , COALESCE(cf.first_order_term_id = t.term_id, FALSE) AS is_first_order
  , t.is_company_holiday AS is_holiday_term
  -- washingtons birthday is the official name for presidents day 
  , ARRAYS_OVERLAP(t.national_holidays, ARRAY_CONSTRUCT('washingtons birthday', 'independence day', 'thanksgiving day', 'christmas day', 'new years eve')) AS is_atypical_retention_week
  , mo.meal_order_id 
  , mo.order_status
  , COALESCE(mo.is_break_skip, FALSE) AS is_break_skip
  , COALESCE(mo.order_status, '') = 'skipped' AS is_skipped
  , is_skipped AND COALESCE(oss.is_forced_skip, FALSE) AS is_forced_skip
  , COALESCE(mo.order_size, 0) AS order_size
  , LAG(order_status) OVER (PARTITION BY ft.customer_id
                            ORDER BY t.term_id) 
    AS prev_order_status
  , SUM(CASE WHEN mo.is_fulfilled THEN 1 ELSE 0 END) OVER 
       (PARTITION BY ft.customer_ID ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    AS running_total_fulfilled_order_count
  , SUM(CASE WHEN mo.is_fulfilled THEN COALESCE(mo.order_size, 0) ELSE 0 END) OVER 
       (PARTITION BY ft.customer_ID ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    AS running_total_fulfilled_meal_count
  , CASE WHEN cohort_week_without_holidays >= 8 
         THEN SUM(CASE WHEN COALESCE(mo.is_fulfilled, FALSE) THEN 1 ELSE 0 END) OVER 
                 (PARTITION BY ft.customer_id ORDER BY t.term_id ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) 
    END AS last_eight_week_order_count
  , mo.is_commitment_order
  , COALESCE(mo.is_fulfilled, FALSE) AS is_fulfilled
  , COALESCE(mo.premium_meal_count > 0, FALSE) AS contains_premium_meal
  , COALESCE(mo.surcharged_meal_count > 0, FALSE) AS contains_surcharged_meal
  , COALESCE(mo.dual_serving_meal_count > 0, FALSE) AS contains_dual_serving_meal
  , COALESCE(mo.breakfast_meal_count > 0, FALSE) AS contains_breakfast_meal
  , COALESCE(mo.autoselection_count > 0, FALSE) AS contains_autoselection
  , uas.user_status AS raw_subscription_status -- Included for testing
  , CASE WHEN order_status IS NOT NULL
         THEN 'active'
         WHEN order_status IS NULL AND raw_subscription_status IS NULL
         THEN 'paused'
         WHEN order_status IS NULL AND raw_subscription_status = 'active'
         -- Since we can't determine when a user was deactivated, set to cancelled
         THEN CASE WHEN c.is_deactivated
                   THEN 'canceled'
                   ELSE 'paused'
              END
         ELSE raw_subscription_status
    END AS subscription_status
  , LAG(subscription_status) OVER (PARTITION BY ft.customer_id
                                   ORDER BY t.term_id) AS prev_subscription_status
  , c.is_deactivated
  , c.is_internal_account
  , c.is_employee
  , COALESCE(cf.is_excluded_from_retention, FALSE) AS is_excluded_from_retention
  , COALESCE(cf.has_excluded_orders_only, FALSE) AS has_excluded_orders_only
  , COALESCE(t.term_id >= cf.cohort, FALSE) AS is_after_cohort_term
  , COALESCE(cf.is_dtc_oven_customer, FALSE) AS is_dtc_oven_customer
  , COALESCE(mo.destination_zip_cd
             , hz.zip_cd
             , LAG(COALESCE(mo.destination_zip_cd, hz.zip_cd)) IGNORE NULLS OVER (PARTITION BY ft.customer_id ORDER BY t.term_id) 
            ) AS zip_cd
  , COALESCE(mo.cycle
             , LAG(mo.cycle) IGNORE NULLS OVER(PARTITION BY ft.customer_id ORDER BY t.term_id)
             ) AS cycle 
  , COALESCE(mo.facility_network
             , LAG(mo.facility_network) IGNORE NULLS OVER(PARTITION BY ft.customer_id ORDER BY t.term_id)
             ) AS facility_network
  , CASE WHEN prev_subscription_status = 'active' AND subscription_status IN ('canceled','paused') 
         THEN CONCAT('active to ',subscription_status) 
         WHEN order_status IN ('skipped','payment_delinquent','payment_error')
         THEN order_status
         ELSE subscription_status
    END AS term_status_type
  , COALESCE(mo.meal_arr, 0) AS meal_arr
  , SUM(CASE WHEN mo.is_fulfilled THEN COALESCE(mo.meal_arr, 0) ELSE 0 END) OVER 
       (PARTITION BY ft.customer_ID ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    AS running_total_fulfilled_meal_revenue
  , COALESCE(mo.meal_arr, 
             CASE WHEN term_status_type NOT IN ('active','canceled','paused') 
                  --customers can skip multiple terms in a row
                  THEN LAG(mo.meal_arr) IGNORE NULLS OVER (PARTITION BY ft.customer_id ORDER BY t.term_id)
                  ELSE 0
             END)
    AS estimated_meal_arr
  , {{ next_or_last_occurence('LAG', 'mo.is_fulfilled', 't.term_id') }} AS last_fulfilled_term_id
  , {{ next_or_last_occurence('LEAD', 'mo.is_fulfilled', 't.term_id') }} AS next_fulfilled_term_id
  , cohort_week_without_holidays 
    - {{ next_or_last_occurence('LAG', 'mo.is_fulfilled', 'cohort_week_without_holidays') }} AS weeks_since_last_order
  , {{ next_or_last_occurence('LEAD', 'mo.is_fulfilled', 'cohort_week_without_holidays') }} 
    -  cohort_week_without_holidays AS weeks_until_next_order
  -- Three possible cases: (1) they aren't skipped this term, so 0 consecutive
  -- (2) they are skipped this term and have had an unskipped order before (find that term, subtract from current term)
  -- (3) they are skipped this term and have never had an unskipped order before (set to NULL)
  , CASE WHEN COALESCE(mo.order_status, '') <> 'skipped'
         THEN 0
         ELSE cohort_week_without_holidays 
              - {{ next_or_last_occurence('LAG', "COALESCE(mo.order_status, '') <> 'skipped'", 'cohort_week_without_holidays') }}
     END AS consecutive_skip_count
  -- Same as consecutive_skip_count, but only cases 1 and 2 apply (since they can't be inactive during their first term)
  , CASE WHEN subscription_status = 'active'
         THEN 0
         ELSE cohort_week_without_holidays 
              - {{ next_or_last_occurence('LAG', "subscription_status = 'active'", 'cohort_week_without_holidays') }}
     END AS consecutive_inactive_count
  , CASE WHEN NOT COALESCE(mo.is_fulfilled, FALSE) 
         THEN 0
         ELSE cohort_week_without_holidays 
              - {{ next_or_last_occurence('LAG', "NOT COALESCE(mo.is_fulfilled, FALSE)", 'cohort_week_without_holidays') }}
     END AS consecutive_fulfilled_count
  , COALESCE(ra.term_subscription_refund_amount, 0) AS term_subscription_refund_amount
  , SUM(COALESCE(ra.term_subscription_refund_amount, 0)) OVER
       (PARTITION BY ft.customer_id ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS running_total_subscription_refund_amount
  , SUM(COALESCE(ra.term_meal_refund_count, 0)) OVER
       (PARTITION BY ft.customer_id ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS running_total_meal_refund_count
  , COALESCE(ra.term_meal_refund_count>0, FALSE) AS term_has_refund
  , COUNT_IF(term_has_refund) OVER
       (PARTITION BY ft.customer_id ORDER BY t.term_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS running_total_refunded_terms 
  , ROW_NUMBER() OVER (PARTITION BY ft.customer_id ORDER BY t.term_id desc) = 1 AS is_most_recent_record
  , NOT COALESCE(mo.is_fulfilled, FALSE) 
      AND NOT c.is_deactivated
      AND NOT c.is_internal_account
      AND weeks_since_last_order = 4
      AND running_total_fulfilled_order_count > 6
      AND is_most_recent_record AS is_reactivation_eligible
  , COALESCE(mo.is_fulfilled, FALSE)
      AND NOT c.is_internal_account
      AND weeks_since_last_order = 4
      AND is_most_recent_record AS is_off_inactive
FROM first_term ft 
INNER JOIN {{ ref('terms') }} t 
  ON ft.first_term <= t.term_id 
  AND  t.order_by_time < {{ current_timestamp_utc() }}  
LEFT JOIN {{ ref('meal_order_facts') }} mo
  ON t.term_id = mo.term_id
  AND  ft.customer_id = mo.customer_id
LEFT JOIN {{ ref('customer_facts') }} cf
  ON ft.customer_id = cf.customer_id
LEFT JOIN {{ ref('cohorts') }} coh
  ON t.term_id = coh.term_id
LEFT JOIN {{ ref('historic_zips') }} hz
  ON hz.customer_id = ft.customer_id
  AND  COALESCE(mo.order_time, t.order_by_time) >= hz.zip_start_time
  AND  COALESCE(mo.order_time, t.order_by_time) < COALESCE(hz.zip_end_time, '9999-12-31') 
LEFT JOIN {{ ref('user_activity_statuses') }} uas
  ON ft.customer_id = uas.customer_id
  -- If there was a meal order, check status at that time ELSE use the term order by time
  AND  COALESCE(mo.order_time, t.order_by_time) >= uas.status_start_time
  AND  COALESCE(mo.order_time, t.order_by_time) < COALESCE(uas.status_end_time, {{ current_timestamp_utc() }})
LEFT JOIN {{ ref('customers') }} c
  ON ft.customer_id = c.customer_id 
LEFT JOIN refund_amounts ra
  ON ft.customer_id = ra.customer_id
  AND t.term_id = ra.affected_term_id
LEFT JOIN {{ ref('ordinal_skip_statuses') }} oss 
  ON oss.customer_id = ft.customer_id 
  AND oss.term_id = t.term_id
  AND oss.row_number = 1
WHERE NOT t.has_no_orders