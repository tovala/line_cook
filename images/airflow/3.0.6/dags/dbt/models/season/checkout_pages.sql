
SELECT
  page_visit_id 
  , session_id
  , base_page
  , base_path
  , FIRST_VALUE(page_visit_start_time) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence) AS checkout_start_time
  , page_visit_start_time 
  , ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence) AS step_order
  , page_visit_duration_seconds AS step_duration_seconds
  , LEAD(base_page) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence) AS next_checkout_page
  , LEAD(base_path) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence) AS next_checkout_path
  , LEAD(event_sequence) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence) AS next_checkout_event_sequence
  --Determine whether or not the session continues in checkout, or is disrupted by other non-checkout pages
  , COALESCE(next_page_visit_event_sequence = next_checkout_event_sequence, FALSE) AS was_next_page_in_checkout
  , next_page_visit_event_sequence IS NULL AS has_exited_site
  , NOT was_next_page_in_checkout AS has_exited_checkout
  --Attribute a session with whether or not the user visited meals-only page for retail signup
  , COUNT(CASE WHEN base_path LIKE 'meals-only%' THEN page_visit_id END) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) > 0 AS contains_meals_only
  , COUNT(CASE WHEN page_url ILIKE '%referral%' THEN page_visit_id END) OVER (PARTITION BY session_id ORDER BY page_visit_start_time, event_sequence ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) > 0 AS contains_referral
FROM {{ table_reference('page_visits', 'grind') }}
WHERE is_checkout_page
