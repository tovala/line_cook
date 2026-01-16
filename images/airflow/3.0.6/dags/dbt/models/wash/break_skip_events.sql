
WITH start_and_end_breaks AS (
  SELECT 
    his1.break_id
    , his1.user_id AS customer_id 
    , his1.created AS break_time
    , his2.created AS raw_unbreak_time
    , his1.start_term
    , his1.end_term
  FROM {{ table_reference('user_subscription_break_history') }} his1
  LEFT JOIN {{ table_reference('user_subscription_break_history') }} his2
    ON his1.break_id = his2.break_id
    AND his2.action = 'deleted'
  WHERE his1.action = 'created'
)
SELECT
  {{ hash_natural_key('sab.break_id', 'num.integer_value') }} AS skip_id 
  , sab.break_id -- For debugging purposes
  , sab.customer_id
  , num.integer_value AS term_id 
  , sab.start_term as break_start_term
  , sab.end_term as break_planned_end_term 
  , sab.break_time 
  -- Breaks will be removed iff:
    -- 1. A customer removes the break
    -- 2. The customer cancels their account
  -- This accounts for breaks that are removed after one or more of the terms has passed 
  , CASE WHEN COALESCE(uto.status, '') = 'on_break' 
              -- The unbreak_time can be non_null if the order time has passed and THEN they remove the break
              AND COALESCE(uto.created, '9999-12-31') < COALESCE(sab.raw_unbreak_time, '9999-12-31')
         THEN NULL 
         ELSE sab.raw_unbreak_time
    END AS unbreak_time
  , CASE WHEN unbreak_time IS NULL 
         THEN 'skipped'
         ELSE 'unskipped'
    END AS skip_end_type
  , FALSE AS is_forced_skip
  , skip_end_type = 'skipped' 
    AND num.integer_value = {{ live_term() }} AS is_on_break 
FROM start_and_end_breaks sab
LEFT JOIN {{ source('brine', 'dim_numbers') }} num 
  ON num.integer_value BETWEEN sab.start_term AND sab.end_term 
LEFT JOIN {{ table_reference('user_term_order') }} uto
  ON sab.customer_id = uto.userid 
  AND num.integer_value = uto.termid 
