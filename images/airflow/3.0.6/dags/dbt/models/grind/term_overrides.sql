
SELECT 
  uts.id AS override_id
  , uts.userid AS customer_id
  , uts.termid AS term_id
  , st.cycle AS override_cycle
  , mca.meal_count AS override_order_size
  , uts.created AS override_time
FROM {{ table_reference('user_term_status') }} uts
LEFT JOIN {{ ref('subterms') }} st
  ON uts.subterm_id = st.subterm_id
LEFT JOIN {{ table_reference('mealcountsallowed') }} mca
  ON uts.mealcountsallowed_id = mca.id
