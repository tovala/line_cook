
  SELECT 
    uas.status_id AS cancellation_id
    , uas.customer_id
    , uas.status_start_time AS cancellation_time
    , uas.status_end_time AS uncancel_time
    , cr.will_order_in_future
    , cr.cancel_reason
    , {{ clean_churn_reason('cr.cancel_reason') }} AS cancel_reason_group
    , cr.customer_typed_reason
    , cr.survey_time
    , cr.landing_id
    , cr.landing_id IS NOT NULL AS has_cancellation_survey
    , t.term_id - fmo.term_id AS weeks_since_first_fulfilled_order
    , COALESCE(cts.running_total_fulfilled_order_count,0) AS fulfilled_order_count
    , LAG(uas.status_start_time) OVER (PARTITION BY uas.customer_id ORDER BY uas.status_start_time) AS prev_cancel_time
    , DATEDIFF('day', cancellation_time, uncancel_time) AS days_since_last_churn
    , ROW_NUMBER() OVER (PARTITION BY uas.customer_id ORDER BY uas.status_start_time) - 1 AS prior_churn_count
  FROM {{ ref('user_activity_statuses') }} uas
  LEFT JOIN {{ ref('cancellation_survey') }} cr
    ON cr.customer_id = uas.customer_id 
    AND cr.survey_time >= uas.status_start_time 
    AND cr.survey_time < COALESCE(uas.status_end_time, '9999-12-31')
  LEFT JOIN {{ ref('first_meal_orders') }} fmo
    ON uas.customer_id = fmo.customer_id
  LEFT JOIN {{ ref('terms') }} t
    ON uas.status_start_time BETWEEN t.start_date AND t.next_term_start_date
  LEFT JOIN {{ ref('customer_term_summary') }} cts
    ON uas.customer_id = cts.customer_id
    AND CASE WHEN t.has_no_orders THEN t.term_id -1 ELSE t.term_id END = cts.term_id --if a customer completed their survey during term 260 then they are missing in term_summary
  WHERE uas.user_status = 'canceled'
  -- New users are added to the DB as canceled so we want to exclude these from our table
    AND NOT uas.is_first_status  
  -- Deduping cancellation surveys
  QUALIFY ROW_NUMBER() OVER (PARTITION BY uas.customer_id, uas.status_id ORDER BY cr.survey_time) = 1
