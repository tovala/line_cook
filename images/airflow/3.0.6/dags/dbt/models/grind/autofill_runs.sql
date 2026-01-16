WITH af_run_info AS (
  SELECT
    run_id
    , run_time
    , test_name
    , subterm_id
    , term_id
    , cycle
    , facility_network
    , meals_per_round
  FROM {{ table_reference('autofill_run_results', 'wash') }}
),

meal_assignments_agg AS (
  SELECT 
    run_id
    , COUNT(*) AS count_meals_assigned
    , COUNT(CASE WHEN ARRAY_CONTAINS('lowerProdBuffer'::VARIANT, targets_bypassed) THEN 1 END) AS count_lower_prod_buffer
    , COUNT(CASE WHEN ARRAY_CONTAINS('dualServingLimit'::VARIANT, targets_bypassed) THEN 1 END) AS count_dual_serving_limit
    , COUNT(CASE WHEN ARRAY_CONTAINS('anticombos'::VARIANT, targets_bypassed) THEN 1 END) AS count_anticombos_assigned
  FROM {{ table_reference('autofill_meal_assignments', 'grind') }}
  GROUP BY run_id
),

user_runs_agg AS (
  SELECT 
    run_id
    , SUM(order_size - count_manual_selections) AS count_meals_needed
    , COUNT(DISTINCT CASE WHEN NOT filled THEN customer_id END) AS count_unfilled_customers
    , AVG(avg_affinity) AS average_affinity
    , STDDEV(avg_affinity) AS stdev_affinity
  FROM {{ table_reference('autofilled_customers', 'grind') }}
  GROUP BY run_id
),
prod_counts_agg AS (
  SELECT 
    run_id
    , SUM(CASE WHEN leftover_meals > 0 THEN leftover_meals ELSE 0 END) AS total_leftover_meals
    , SUM(CASE WHEN leftover_meals > 0 AND meal_sku_type = 'regular' THEN leftover_meals ELSE 0 END) AS total_leftover_regular_meals
  FROM {{ ref('autofill_production_counts') }}
GROUP BY ALL
)
SELECT
  af.run_id
  , af.run_time
  , af.test_name
  , af.subterm_id
  , af.term_id
  , af.cycle
  , af.facility_network
  , af.meals_per_round
  , ura.count_unfilled_customers
  , maa.count_meals_assigned
  , ura.count_meals_needed
  , ura.count_meals_needed - maa.count_meals_assigned AS count_unfilled_meals
  , COALESCE(maa.count_meals_assigned = ura.count_meals_needed, FALSE) AS was_successful
  , pca.total_leftover_meals 
  , pca.total_leftover_regular_meals
  , maa.count_lower_prod_buffer
  , maa.count_dual_serving_limit
  , maa.count_anticombos_assigned
  , ura.average_affinity
  , ura.stdev_affinity
  , af.run_time = MAX(af.run_time) OVER (PARTITION BY af.subterm_id) AS was_most_recent
FROM af_run_info af
LEFT JOIN meal_assignments_agg maa
  ON af.run_id = maa.run_id
LEFT JOIN user_runs_agg ura
  ON af.run_id = ura.run_id
LEFT JOIN prod_counts_agg pca 
  ON af.run_id = pca.run_id