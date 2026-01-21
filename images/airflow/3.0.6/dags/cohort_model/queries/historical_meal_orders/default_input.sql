/*
Historical meal and order counts.
These are used to backfill past values with historical actuals.
*/

SELECT
    cts.term_id
    , cts.cohort
    , COUNT(cts.meal_order_id) AS order_count
    , SUM(cts.order_size) AS meal_count
FROM {{ params.parent_database }}.SEASON.CUSTOMER_TERM_SUMMARY AS cts
LEFT JOIN {{ params.parent_database }}.GRIND.MEAL_ORDERS AS mo
    ON cts.meal_order_id = mo.meal_order_id
    AND mo.is_trial_order <> TRUE
WHERE cts.is_fulfilled
    AND cts.is_internal_account <> TRUE
    AND cts.facility_network IS NOT NULL
    AND cts.is_excluded_from_retention <> TRUE
    AND cts.cohort IS NOT NULL
    AND cts.term_id IS NOT NULL
GROUP BY 1,2