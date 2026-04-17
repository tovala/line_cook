WITH initial_by_cohort AS (
  SELECT 
    cohort
    , order_count AS init_order_val 
  FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".HISTORICAL_MEAL_ORDERS
  WHERE term_id = cohort
  UNION
  SELECT 
    cohort
    , init_order_val 
  FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".FUTURE_COHORT_INITIAL_ORDER_PREDICTIONS
  ORDER BY cohort
)
SELECT 
  init_order_val 
FROM initial_by_cohort; -- get only the initial values in order for 1xN vector