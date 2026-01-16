
WITH order_numbers AS (
 SELECT 
   termid 
   , COUNT(*) AS num_orders
 FROM {{ table_reference('user_term_order') }} 
 GROUP BY 1   
),
most_recent_shipped AS (
  SELECT 
    MAX(termid) 
  FROM {{ table_reference('user_term_order') }} 
  WHERE termid <> (SELECT MAX(termid) FROM {{ table_reference('user_term_order') }} )
), 
subterm_ship_date AS (
  SELECT 
    term_id 
    , SUM(CASE WHEN NOT is_available THEN 1 ELSE 0 END) AS unavailable_subterms_count
    , MIN(subterm_ship_time) AS first_ship_time
  FROM {{ ref('subterms') }}
  GROUP BY 1
),
terms_start_dates AS (
  SELECT 
    t.id AS term_id 
    , CASE WHEN DATE_PART('dow', t.start_date::DATE) <> 1 
           THEN DATE_TRUNC('week', t.start_date::DATE)::DATE
           ELSE t.start_date::DATE 
      END AS start_date 
    , COALESCE(LEAD(DATE_TRUNC('week', t.start_date::DATE)::DATE) OVER (ORDER BY t.id), 
               DATE_TRUNC('week', t.start_date::DATE)::DATE + INTERVAL '7 days')::DATE 
      AS next_term_start_date
  FROM {{ table_reference('terms') }} t
  WHERE COALESCE(t.ready_for_view, FALSE)
),
term_holidays AS (
  SELECT 
    tsd.term_id 
    , tsd.start_date 
    , tsd.next_term_start_date 
    , ARRAY_AGG(hol.clean_holiday_name) AS national_holidays
  FROM terms_start_dates tsd 
  LEFT JOIN {{ source('brine', 'holidays') }} hol 
    ON hol.holiday_date >= tsd.start_date 
    AND hol.holiday_date < tsd.next_term_start_date
  GROUP BY 1,2,3
)
SELECT 
  t.id AS term_id
  , t.order_by_date AS order_by_time
  -- If the start_date isn't a Monday, make it Monday
  , th.start_date
  , t.end_date::DATE AS end_date
  -- This is included for ease of joining to this table based on date
  , th.next_term_start_date  
  , o.num_orders IS NULL AS has_no_orders 
  , order_by_time <= {{ current_timestamp_utc() }} AS is_past_order_by
  -- this is a term where we either did not have order OR we had a special limit on orders
  , (has_no_orders AND is_past_order_by) OR t.id IN (155,156) AS is_company_holiday
  , NOT is_past_order_by 
    AND {{ current_timestamp_utc() }} >= LAG(order_by_time) OVER (ORDER BY t.id) AS is_live
  , {{ current_timestamp_utc() }} < LEAD(order_by_time) OVER (ORDER BY t.id) 
    AND is_past_order_by AS is_being_produced
  , first_ship_time <= {{ current_timestamp_chicago() }}::DATE 
    AND t.end_date::DATE >= {{ current_timestamp_chicago() }}::DATE AS is_being_shipped
  -- Order finalization at midnight of following day
  , DATEADD('hours', 72, order_by_time) > {{ current_timestamp_utc() }} AS is_editable
  , t.id = (SELECT * FROM most_recent_shipped) AS is_latest_completed
  , COALESCE(unavailable_subterms_count > 0, FALSE) AS is_partially_unavailable
  , LAG(CASE WHEN NOT is_company_holiday
             THEN order_by_time 
    END) IGNORE NULLS OVER (ORDER BY th.term_id) AS prev_term_order_by_time
  , th.national_holidays
FROM term_holidays th 
LEFT JOIN {{ table_reference('terms') }} t
  ON th.term_id = t.id
LEFT JOIN order_numbers o 
  ON t.id = o.termid 
LEFT JOIN subterm_ship_date ssd
  ON t.id = ssd.term_id
