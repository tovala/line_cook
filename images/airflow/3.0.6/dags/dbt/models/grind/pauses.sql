
  SELECT 
    uas.status_id AS pause_id
    , uas.customer_id
    , uas.status_start_time AS pause_time
    , uas.status_end_time AS unpause_time
    , pr.will_order_in_future
    , pr.pause_reason
    , {{ clean_churn_reason('pr.pause_reason') }} AS pause_reason_group
    , pr.customer_typed_reason
    , pr.survey_time
    , pr.landing_id
    , pr.landing_id IS NOT NULL AS has_pause_survey
    , t.term_id - fmo.term_id AS weeks_since_first_fulfilled_order
    , COALESCE(cts.running_total_fulfilled_order_count,0) AS fulfilled_order_count
    , LAG(uas.status_start_time) OVER (PARTITION BY uas.customer_id ORDER BY uas.status_start_time) AS prev_pause_time
    , DATEDIFF('day', pause_time, unpause_time) AS days_since_last_churn
    , ROW_NUMBER() OVER (PARTITION BY uas.customer_id ORDER BY uas.status_start_time) - 1 AS prior_churn_count
  FROM {{ ref('user_activity_statuses') }} uas
  LEFT JOIN {{ ref('pause_survey') }} pr
  ON pr.customer_id = uas.customer_id 
    AND pr.survey_time >= uas.status_start_time 
    AND pr.survey_time < COALESCE(uas.status_end_time, '9999-12-31')
  LEFT JOIN {{ ref('first_meal_orders') }} fmo
    ON uas.customer_id = fmo.customer_id
  LEFT JOIN {{ ref('terms') }} t
    ON uas.status_start_time BETWEEN t.start_date AND t.next_term_start_date
  LEFT JOIN {{ ref('customer_term_summary') }} cts
    ON uas.customer_id = cts.customer_id
    AND CASE WHEN t.has_no_orders THEN t.term_id -1 ELSE t.term_id END = cts.term_id --if a customer completed their survey during term 260 then they are missing in term_summary
  WHERE uas.user_status = 'paused'
  -- New users are added to the DB as canceled so we want to exclude these from our table
    AND NOT uas.is_first_status  
  --Deduping pause surveys
  QUALIFY ROW_NUMBER() OVER (PARTITION BY uas.customer_id, uas.status_id ORDER BY pr.survey_time) = 1
 