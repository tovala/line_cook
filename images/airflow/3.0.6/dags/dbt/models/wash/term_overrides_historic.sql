
WITH overrides AS (
  SELECT 
    uts.id AS override_id
    , uts.operation AS operation_type
    , uts.userid AS customer_id
    , uts.termid AS term_id
    , st.cycle AS override_cycle
    , mca.meal_count AS override_order_size
    , uts.stamp AS override_start_time
    , LEAD(uts.stamp) OVER (PARTITION BY customer_id, term_id ORDER BY stamp) AS override_end_time
  FROM (
      SELECT 
        utsa9.id 
        , utsa9.operation 
        , utsa9.userid 
        , utsa9.termid 
        , utsa9.stamp 
        , utsa9.mealcountsallowed_id
        , COALESCE(utsa9.subterm_id, s9.id, uts9.subterm_id) AS subterm_id 
      FROM {{ table_reference('user_term_status_audit') }} utsa9
      LEFT JOIN {{ table_reference('user_term_status') }} uts9 
        ON utsa9.id = uts9.id 
      LEFT JOIN {{ table_reference('stripesubscriptions') }} ss9 
        ON utsa9.userid = ss9.userid 
      LEFT JOIN {{ table_reference('subterms') }} s9 
        ON utsa9.termid = s9.termid 
        AND ss9.default_ship_period = s9.ship_period 
  ) uts 
  LEFT JOIN {{ ref('subterms') }} st
    ON uts.subterm_id = st.subterm_id
  LEFT JOIN {{ table_reference('mealcountsallowed') }} mca
    ON uts.mealcountsallowed_id = mca.id)
SELECT 
  override_id
  , customer_id
  , term_id
  , override_cycle
  , override_order_size
  , override_start_time
  , override_end_time
FROM overrides 
WHERE operation_type IN ('INSERT', 'UPDATE')
