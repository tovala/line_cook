
WITH meals_selected AS (
  SELECT 
    ms.term_id
    , ms.customer_id 
    , COUNT(DISTINCT ms.meal_selection_id) AS meals_selected
    , COUNT(DISTINCT CASE WHEN ms.is_autoselection THEN ms.meal_selection_id END) AS autofill_count
    , COUNT(DISTINCT CASE WHEN me.is_premium THEN ms.meal_selection_id END) AS premium_meals_selected
    , COUNT(DISTINCT CASE WHEN me.is_breakfast THEN ms.meal_selection_id END) AS breakfast_meals_selected
  FROM {{ ref('meal_selections') }} ms
  LEFT JOIN {{ ref('meal_skus') }} me
    ON ms.meal_sku_id = me.meal_sku_id
  INNER JOIN {{ ref('terms') }} te 
    ON ms.term_id = te.term_id
  WHERE te.is_editable
  GROUP BY 1,2    
)
SELECT 
  {{ hash_natural_key('te.term_id', 'lus.customer_id', 'te.order_by_time') }} AS future_term_id
  , te.term_id
  , lus.customer_id
  , lus.default_order_size AS default_order_size
  , COALESCE(mor.order_size, tov.override_order_size, lus.default_order_size) AS actual_order_size
  , lus.cycle AS default_cycle
  , COALESCE(mor.cycle, tov.override_cycle, lus.cycle) AS actual_cycle -- Add
  , lus.wants_double_autofill
  , lus.user_status AS latest_user_status
  , COALESCE(us.status, '') = 'skipped' AS is_skipped
  , is_skipped AND COALESCE(us.is_forced_skip, FALSE) AS is_forced_skip
  , is_skipped AND COALESCE(us.is_break_skip, FALSE) AS is_break_skip
  , COALESCE(ms.meals_selected, 0) AS total_meals_count
  , COALESCE(ms.autofill_count, 0) AS autofill_count
  , COALESCE(ms.premium_meals_selected, 0) AS premium_meal_count
  , COALESCE(ms.breakfast_meals_selected, 0) AS breakfast_meal_count
  -- There are rare edge cases where a customer has selected more meals than the actual order size
  , COALESCE(ms.meals_selected, 0) >= actual_order_size AS is_fully_selected
  , fmo.customer_id IS NULL AS is_new_customer
  , COALESCE(cf.eats_pork, FALSE) AS eats_pork
  , COALESCE(cf.eats_tofu, FALSE) AS eats_tofu
  , cus.is_employee
  , cus.is_internal_account
  , cus.is_delinquent 
  , COALESCE(cf.is_inactive, FALSE) AS is_inactive
  -- For any orders past the deadline, the only way a user can be auto-assigned surcharged skus is if their payment status is set to authorized
  , CASE WHEN mor.meal_order_id IS NOT NULL
         THEN COALESCE(pay.payment_status, '') = 'authorized'
         ELSE COALESCE(cf.wants_surcharged_autofill, FALSE) 
    END AS wants_surcharged_autofill
  , COALESCE(fus.wants_breakfast_autofill, cf.wants_breakfast_autofill, FALSE) AS wants_breakfast_autofill
  , add.zip_cd
  , uss.facility_network
  , uss.subterm_id
  , uss.shipping_origin
  , uss.shipping_company
  , uss.shipping_service
  , te.is_past_order_by
FROM {{ ref('terms') }} te
FULL OUTER JOIN {{ ref('latest_user_statuses') }} lus 
LEFT JOIN {{ ref('term_overrides') }} tov 
  ON lus.customer_id = tov.customer_id
  AND te.term_id = tov.term_id
LEFT JOIN {{ ref('upcoming_skips') }} us 
  ON lus.customer_id = us.customer_id
  AND te.term_id = us.term_id
LEFT JOIN meals_selected ms
  ON lus.customer_id = ms.customer_id
  AND te.term_id = ms.term_id
LEFT JOIN {{ ref('first_meal_orders') }} fmo
  ON lus.customer_id = fmo.customer_id
LEFT JOIN {{ ref('customer_facts') }} cf
  ON lus.customer_id = cf.customer_id
LEFT JOIN {{ ref('customers') }} cus
  ON lus.customer_id = cus.customer_id
LEFT JOIN {{ ref('meal_orders') }} mor
  ON lus.customer_id = mor.customer_id 
  AND te.term_id = mor.term_id
LEFT JOIN {{ ref('full_user_statuses') }} fus
  ON mor.customer_id = fus.customer_id
  AND mor.order_time >= fus.status_start_time
  AND mor.order_time < COALESCE(fus.status_end_time, '9999-12-31')
LEFT JOIN {{ ref('payments') }} pay
  ON mor.payment_id = pay.payment_id
LEFT JOIN {{ ref('addresses') }} add
  ON lus.customer_id = add.customer_id
LEFT JOIN {{ ref('upcoming_subterm_shipping') }} uss
  ON uss.zip_cd = add.zip_cd 
  AND uss.term_id = te.term_id
  AND uss.cycle = COALESCE(mor.cycle, tov.override_cycle, lus.cycle)
WHERE te.is_editable
  AND ((te.has_no_orders AND lus.user_status = 'active') 
        OR (NOT te.has_no_orders AND COALESCE(mor.is_fulfilled, FALSE)))
