
SELECT
  mr.menu_rating_id
  , mr.customer_id
  , mr.rating_time
  , mr.menu_rating
  , mr.source_os
  , mr.term_id
  , mr.user_comment
  , lf.current_facility_network
  , CASE WHEN cts.cohort_week_without_holidays < 9 
         THEN TRUE 
         ELSE FALSE 
    END AS is_first_eight_weeks
  , cts.term_status_type
FROM {{ ref('menu_ratings') }} mr
LEFT JOIN {{ ref('latest_facility_per_customer') }} lf
  ON mr.customer_id = lf.customer_id
LEFT JOIN {{ ref('customer_term_summary') }} cts
  ON mr.customer_id = cts.customer_id
  AND mr.term_id = cts.term_id
