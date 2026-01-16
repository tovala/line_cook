
WITH full_user_statuses AS (
  SELECT 
    sa.id AS status_id
    , sa.userid AS customer_id
    , CASE WHEN tf.timestamp_change IS NOT NULL
           THEN DATEADD(SECOND, tf.timestamp_change, sa.created)
           ELSE sa.created 
      END AS status_start_time
    , sa.typeid AS subscription_type
    , sa.default_meal_count AS default_order_size
    , sa.double AS wants_double_autofill
    , sa.default_ship_period AS cycle
    , COALESCE(sa.autofill_breakfast_ok, FALSE) AS wants_breakfast_autofill
    , COALESCE(sa.autofill_surcharge_ok, FALSE) AS wants_surcharged_autofill
    , CASE WHEN sa.active
           THEN 'active' 
           WHEN NOT sa.active AND sa.typeid IS NULL
           THEN 'canceled'
           WHEN NOT sa.active AND sa.typeid IS NOT NULL
           THEN 'paused'
        END AS user_status
    , CASE WHEN sa.commitment
           THEN 'committed'
           ELSE 'not_committed'
      END AS on_commitment
    -- Sometimes we get identical rows at exactly the same time. Jane got tired of fixing this. 
    , ROW_NUMBER() OVER (PARTITION BY customer_id, status_start_time, subscription_type, default_order_size
                                     , wants_double_autofill, cycle, wants_breakfast_autofill, wants_surcharged_autofill
                                     , user_status, on_commitment
                         ORDER BY status_id) AS row_deduper
  FROM {{ table_reference('subscriptionactivity') }} sa
  INNER JOIN {{ ref('leads') }} l ON sa.userid = l.lead_id
  LEFT JOIN {{ source('brine','subscriptionactivity_timestamp_fixes') }} tf
    ON tf.sa_id = sa.id
  -- Exclude rows that we've marked for exclusion in brine table
  WHERE NOT COALESCE(tf.remove_row, FALSE))
SELECT 
  status_id
  , customer_id
  , status_start_time
  , LEAD(status_start_time) OVER (PARTITION BY customer_id ORDER BY status_start_time) AS status_end_time
  , subscription_type
  , CASE WHEN user_status IN ('paused', 'canceled') 
         THEN default_order_size
         ELSE COALESCE(default_order_size, 
                       LEAD(default_order_size) IGNORE NULLS OVER (PARTITION BY customer_id, user_status 
                                                                   ORDER BY status_start_time))
    END AS default_order_size
  , wants_double_autofill
  , cycle
  , wants_breakfast_autofill
  , wants_surcharged_autofill
  , user_status
  , on_commitment
  , LAG(user_status) OVER (PARTITION BY customer_id
                           ORDER BY status_start_time) AS prev_user_status 
  , LAG(on_commitment) OVER (PARTITION BY customer_id
                             ORDER BY status_start_time) AS prev_on_commitment
FROM full_user_statuses
WHERE row_deduper = 1
