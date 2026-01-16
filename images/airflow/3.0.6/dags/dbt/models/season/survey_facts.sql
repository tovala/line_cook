-- Facts table for ALL survey responses by customer 
-- Pulls together additional customer details that could add valuable insight on top of survey responses

WITH autoselected_meals AS (
    SELECT 
        customer_id 
        , COUNT(DISTINCT meal_selection_id) AS autoselected_meals
    FROM {{ ref('meal_selections') }} 
    WHERE is_autoselection
    GROUP BY 1 
)
, -- Pulls the latest term before the current term that actually had orders 
latest_term AS (
  SELECT 
      t.term_id AS term_id
      , (LEAD(t.term_id, -1) OVER (PARTITION BY t.has_no_orders ORDER BY t.term_id)) AS latest_term_id
  FROM {{ ref('terms') }} t 
  WHERE NOT t.has_no_orders 
  ORDER BY t.term_id DESC
)
, -- Pulls earliest cook times for a customer for both scanning store and chef's recipe
earliest_cooks AS (
  SELECT 
    customer_id
    , MIN(CASE WHEN cook_event_type = 'scan_the_store' THEN cumulative_earliest_cook_cycle_timestamp END) AS first_scan_start_time
    , MIN(CASE WHEN cook_event_type = 'chefs_recipe' THEN cumulative_earliest_cook_cycle_timestamp END) AS first_recipe_start_time
  FROM {{ ref('customer_oven_usage_totals') }}
  GROUP BY 1
)
, -- Calculates the time a CS ticket was first opened for a customer 
cs_tickets AS (
  SELECT 
    csu.customer_id
  , MIN(initial_ticket_assigned_time) AS first_ticket_time
  FROM {{ ref('cs_users') }} csu
  INNER JOIN {{ ref('cs_tickets') }} cst
    ON csu.zendesk_user_id = cst.requester_id
  GROUP BY 1
)
-- Statuses, order counts, and meal counts are all calculated based on when the survey was completed
-- i.e. these will NOT necessarily be the most recent updates for a customer
SELECT 
    t.response_id -- Takes landing_id from all Typeform surveys and renames 
    , t.customer_id AS customer_id 
    , t.response_time AS survey_time
    -- User statuses and details at the time the survey was taken 
    , fus.user_status AS subscription_status
    , fus.default_order_size AS default_order_size
    , fus.cycle AS default_cycle
    , COALESCE(cts.is_break_skip, FALSE) AS was_on_break
    -- Customer facts to gather first order info 
    , c.first_shipping_service
    , CASE WHEN c.first_meal_order_time IS NOT NULL 
                 THEN DATEDIFF(week, c.first_meal_order_time, t.response_time) 
                 ELSE NULL 
      END AS weeks_since_first_order
    -- Latest shipping details 
    , c.latest_shipping_company AS most_recent_line_haul
    , c.latest_shipping_service AS most_recent_shipping_service
    , c.latest_shipping_days AS most_recent_shipping_days
    -- Customer term details at the time the survey was taken
    , a.autoselected_meals AS fulfilled_autoselected_count
    , cts.last_eight_week_order_count AS last_eight_week_order_count
    , cts.facility_network AS default_facility_network
    -- Flags for customer actions taken by the time the survey was created
    , COALESCE(a.autoselected_meals>0, FALSE) AS had_autoselected_meal
    , COALESCE(cst.first_ticket_time,'9999-12-31') <= t.response_time AS had_contacted_cs
    , COALESCE(ec.first_scan_start_time,'9999-12-31') <= t.response_time AS had_scanned_the_store
    , COALESCE(ec.first_recipe_start_time,'9999-12-31') <= t.response_time AS had_cooked_chefs_recipe
    -- Customers who haven't received an order before taking the survey will have null values for these fields 
    , COALESCE(cts.running_total_fulfilled_order_count,0) AS fulfilled_order_count
    , COALESCE(cts.running_total_fulfilled_meal_count,0) AS fulfilled_meal_count
FROM {{ ref('all_typeform_responses') }} t 
LEFT JOIN {{ ref('full_user_statuses') }} fus
    ON t.customer_id = fus.customer_id
    -- Pull statuses that occurred at the time of the survey response 
    AND t.response_time >= fus.status_start_time
    AND t.response_time < COALESCE(fus.status_end_time,'9999-12-31')
LEFT JOIN {{ ref('customer_facts') }} c
    ON t.customer_id = c.customer_id 
LEFT JOIN {{ ref('terms') }} terms 
    -- Pull the term in which the customer took the survey
    ON t.response_time >= terms.start_date
    AND t.response_time < terms.next_term_start_date
-- Pull the id of the term with orders that is closest to when the survey was taken 
-- This will ensure that results from customer_term_summary are the most recent at time of survey 
LEFT JOIN latest_term lt ON terms.term_id = lt.term_id 
LEFT JOIN {{ ref('customer_term_summary') }} cts 
    ON t.customer_id = cts.customer_id
    -- Pull customer term summaries that occurred on or after the time of the survey response 
    AND lt.latest_term_id = cts.term_id 
    -- Any terms that have has_no_orders set will not show up in the customer_term_summary by design
    -- As a result, we pull the closest term with orders to compare with the survey term 
LEFT JOIN autoselected_meals a
    ON t.customer_id = a.customer_id
LEFT JOIN earliest_cooks ec
    ON t.customer_id = ec.customer_id
LEFT JOIN cs_tickets cst
    ON t.customer_id = cst.customer_id
