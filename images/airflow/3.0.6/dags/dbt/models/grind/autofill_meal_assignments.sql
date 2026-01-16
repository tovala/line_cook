{{ 
    config(
        materialized='incremental',
        unique_key='autofill_meal_assignment_id',
        tags=["thyme_incremental"]
    ) 
}}

WITH all_meal_assignments AS (
  SELECT
    run_id 
    , subterm_id
    , run_time
    , test_name
    , ma.index AS meal_assignment_key
    , cu.value AS user_data
    , ma.value AS meal_assignment_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => completed_users) cu,
  LATERAL FLATTEN(input => cu.value:AutofillSelections) ma
  {% if is_incremental() %}
  WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
  {% endif %}

  UNION

  SELECT
    run_id 
    , subterm_id
    , run_time
    , test_name
    , ma.index AS meal_assignment_key
    , cu.value AS user_data
    , ma.value AS meal_assignment_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => unfilled_users) cu,
  LATERAL FLATTEN(input => cu.value:AutofillSelections) ma
  {% if is_incremental() %}
  WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
  {% endif %}
)

SELECT
  run_id
  , subterm_id
  , run_time  
  , test_name
  , {{ hash_natural_key('run_id', 'user_data:ID') }} AS customer_run_id 
  , AS_INTEGER(user_data:ID) AS customer_id
  , AS_INTEGER(meal_assignment_data:MealID) AS meal_sku_id
  , meal_assignment_key
  , {{ hash_natural_key('run_id', 'customer_id', 'meal_assignment_key', 'meal_sku_id') }} AS autofill_meal_assignment_id
  , AS_INTEGER(meal_assignment_data:Round) AS round
  , AS_INTEGER(meal_assignment_data:AffinityThreshold) AS affinity_threshold
  , TRY_TO_BOOLEAN({{ clean_string('meal_assignment_data:AllowAnticombos::STRING') }}) AS allow_anticombos
  , TRY_TO_BOOLEAN({{ clean_string('meal_assignment_data:IncreasedDualLimit::STRING') }}) AS increased_dual_limit
  , ARRAY_CONTAINS('lowerProdBuffer'::VARIANT, meal_assignment_data:TargetsBypassed) AS lowered_production_buffer 
  , {{ clean_string('meal_assignment_data:MealSet') }} AS meal_set 
  , meal_assignment_data:TargetsBypassed AS targets_bypassed
  , user_data:MealAffinity[AS_INTEGER(meal_assignment_data:MealID)::VARCHAR] AS meal_affinity
FROM all_meal_assignments 
