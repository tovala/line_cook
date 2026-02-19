-- WEEKLY MEAL AND MEAL ORDER COUNTS
-- "historical_meal_orders" input in order_projections.py

/* 
In the current process, Emily pulls these weekly from 
the "Weekly Meal Count by Cohort" and
"Weekly Order Count by Cohort" tabs of the
Sigma workbook "Demand Planning (Trying to Add a Control for Total/D2C/Costco/etc)"
(https://app.sigmacomputing.com/tovala/workbook/workbook-7rOdCuzCz8YKLBjWYX7tlO?:link_source=share).

Daniel wrote the code below to create these inputs as allspice.historical_meal_orders
(https://github.com/tovala/spice_rack/pull/530/changes#diff-a8b1223b51f49ed6414244db1af734e0aece36a8ed2401f9ff059528185fb97d)
but I don't think that PR ever got deployed. 
*/

SELECT
    cts.term_id
    , cts.cohort
    , COUNT(cts.meal_order_id) AS order_count
    , SUM(cts.order_size) AS meal_count
FROM season.customer_term_summary AS cts 
LEFT JOIN grind.meal_orders AS mo
    ON cts.meal_order_id = mo.meal_order_id
    AND mo.is_trial_order <> TRUE
WHERE cts.is_fulfilled
    AND cts.is_internal_account <> TRUE
    AND cts.facility_network IS NOT NULL
    AND cts.is_excluded_from_retention <> TRUE
    AND cts.cohort IS NOT NULL
    AND cts.term_id IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2