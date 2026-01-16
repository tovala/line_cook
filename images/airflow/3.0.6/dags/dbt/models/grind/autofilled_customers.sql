{{ 
    config(
        materialized='incremental',
        unique_key='customer_run_id',
        tags=["thyme_incremental"]
    ) 
}}

WITH autofill_assignments AS (
  SELECT
    customer_run_id,
    meal_sku_id
  FROM {{ table_reference('autofill_meal_assignments', 'grind') }} 
  {% if is_incremental() %}
  WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
  {% endif %}
),

all_user_data AS (
  SELECT
    run_id
    , subterm_id
    , run_time
    , test_name
    , TRUE AS filled
    , {{ hash_natural_key('run_id', 'cu.value:ID') }} AS customer_run_id
    , cu.value AS user_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => completed_users) cu
  {% if is_incremental() %}
  WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
  {% endif %}

  UNION

  SELECT
    run_id
    , subterm_id
    , run_time
    , test_name
    , FALSE AS filled
    , {{ hash_natural_key('run_id', 'uu.value:ID') }} AS customer_run_id
    , uu.value AS user_data
  FROM {{ table_reference('autofill_run_results', 'wash') }},
  LATERAL FLATTEN(input => unfilled_users) uu
  {% if is_incremental() %}
  WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
  {% endif %}
)
SELECT 
  aud.run_id
  , aud.subterm_id
  , aud.customer_run_id
  , aud.run_time
  , aud.filled
  , aud.user_data:ID AS customer_id
  , AS_INTEGER(aud.user_data:MaxSelections) AS order_size
  , TRY_TO_BOOLEAN({{ clean_string('aud.user_data:AllowDualSelections::STRING') }}) AS allow_dual_selections
  , TRY_TO_BOOLEAN({{ clean_string('aud.user_data:AllowBreakfast::STRING') }}) AS allow_breakfast
  , TRY_TO_BOOLEAN({{ clean_string('aud.user_data:AllowSurcharge::STRING') }}) AS allow_surcharge
  , CASE WHEN COALESCE(ARRAY_SIZE(aud.user_data:ManualSelections), 0) = 0
         THEN NULL
         ELSE aud.user_data:ManualSelections 
    END AS manual_selections
  , COALESCE(ARRAY_SIZE(aud.user_data:ManualSelections), 0) AS count_manual_selections
  , aud.user_data:AutofillSelections AS autofill_selections
  , COALESCE(ARRAY_SIZE(aud.user_data:AutofillSelections), 0) AS count_autofill_selections
  , aud.user_data:MealAnticombos AS meal_anticombinations
  , AVG(aud.user_data:MealAffinity[aa.meal_sku_id::VARCHAR]) AS avg_affinity
FROM all_user_data aud
LEFT JOIN autofill_assignments aa ON aud.customer_run_id = aa.customer_run_id
GROUP BY ALL
