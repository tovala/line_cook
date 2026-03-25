/*
Historical meal and order counts.
These are used to backfill past values with historical actuals.
*/
CREATE OR REPLACE TABLE {{ params.database }}.{{ params.temp_schema }}.historical_meal_orders
AS
  SELECT
      cts.term_id
      , cts.cohort
      , COUNT(cts.meal_order_id) AS order_count
      , SUM(cts.order_size) AS meal_count
  FROM {{ params.database }}.SEASON.CUSTOMER_TERM_SUMMARY AS cts
  LEFT JOIN {{ params.database }}.GRIND.MEAL_ORDERS AS mo
      ON cts.meal_order_id = mo.meal_order_id
      AND mo.is_trial_order <> TRUE
  WHERE cts.is_fulfilled
      AND cts.is_internal_account <> TRUE
      AND cts.facility_network IS NOT NULL
      AND cts.is_excluded_from_retention <> TRUE
      AND cts.cohort IS NOT NULL
      AND cts.term_id IS NOT NULL
  GROUP BY 1,2;