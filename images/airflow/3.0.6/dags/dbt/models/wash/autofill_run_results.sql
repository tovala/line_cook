{{ 
    config(
        materialized='incremental',
        unique_key='run_id',
        tags=["thyme_incremental"]
    ) 
}}

SELECT 
  {{ hash_natural_key('run_time', 'ar.term_id', 'ar.cycle', 'ar.facility_network') }} AS run_id 
  , ar.run_time 
  , ar.term_id 
  , ar.cycle
  , ar.facility_network 
  , st.subterm_id 
  , ar.meals_per_round
  , ar.raw_data:TestName AS test_name
  , ar.raw_data:CompletedAutoFillUsers AS completed_users 
  , ar.raw_data:UnfilledUsers AS unfilled_users 
  , ar.raw_data:LeftoverMeals AS leftover_meals
  , ar.raw_data:SoldOutMeals AS soldout_meals
  , ar.raw_data:UnassignableMeals AS unassignable_meals
FROM {{source('chili', 'autofill_results')}} ar
LEFT JOIN {{ ref('subterms') }} st 
  ON ar.term_id = st.term_id 
  AND ar.facility_network = st.facility_network
  AND ar.cycle = st.cycle

{% if is_incremental() %}
WHERE run_time >= (SELECT MAX(run_time) FROM {{ this }})
{% endif %}