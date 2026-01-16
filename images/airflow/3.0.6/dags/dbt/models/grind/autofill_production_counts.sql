  WITH all_production_counts AS (
  SELECT
    run_id
    , subterm_id
    , run_time
    , test_name
    , 'unassigned' AS assignment_type
    , ua.value as production_count_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => unassignable_meals) ua

  UNION

  SELECT
    run_id
    , subterm_id
    , run_time
    , test_name
    , 'sold_out' AS assignment_type
    , so.value as production_count_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => soldout_meals) so

  UNION

  SELECT
    run_id
    , subterm_id
    , run_time
    , test_name
    , 'leftover' AS assignment_type
    , lo.value as production_count_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => leftover_meals) lo
)

SELECT
  apc.run_id
  , apc.subterm_id
  , apc.run_time
  , apc.test_name
  , apc.assignment_type
  , {{ hash_natural_key('apc.run_id', 'apc.production_count_data:ID') }} AS autofill_production_count_id 
  , AS_INTEGER(apc.production_count_data:ID) AS meal_sku_id 
  , AS_INTEGER(apc.production_count_data:ProductionCode) AS production_cd 
  , {{ clean_string('apc.production_count_data:Type') }} AS raw_type
  , CASE WHEN raw_type = 'breakfast'
         THEN 'breakfast'
         WHEN raw_type = 'regular' AND AS_INTEGER(apc.production_count_data:SurchargeAmount) > 0
         THEN 'surcharged'
         WHEN sku.contains_pork
         THEN 'pork'
         ELSE raw_type 
    END AS meal_sku_type
  , AS_INTEGER(apc.production_count_data:ProductionCount) AS production_count
  , AS_INTEGER(apc.production_count_data:HeldCount) AS held_count
  , AS_INTEGER(apc.production_count_data:SelectedAmmount) AS assigned_count
  , AS_INTEGER(apc.production_count_data:SoldOutAmount) AS sold_out_amount
  , CEIL(GREATEST(production_count*.0025,6)) AS lowest_buffer
  , production_count - lowest_buffer - assigned_count AS leftover_meals
  , apc.production_count_data:AntiCombos AS anticombos 
  , AS_INTEGER(apc.production_count_data:SurchargeAmount) AS surcharge_amount
  , AS_INTEGER(apc.production_count_data:AssignmentPriority) AS assignment_priority
  -- Count of how many folks had affinity = 0 for this meal
  , AS_INTEGER(apc.production_count_data:DifficultyRating) AS difficulty_rating
FROM all_production_counts apc
LEFT JOIN {{ ref('meal_skus') }} sku 
  ON AS_INTEGER(apc.production_count_data:ID) = sku.meal_sku_id 
