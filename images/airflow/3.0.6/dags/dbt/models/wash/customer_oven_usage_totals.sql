SELECT 
  customer_id
  , cook_event_type
  , SUM(total_cooks) AS sum_total_cooks 
  , SUM(total_completed_cooks) AS sum_total_completed_cooks
  , MAX(latest_cook_cycle_timestamp) AS cumulative_latest_cook_cycle_timestamp
  , MIN(earliest_cook_cycle_timestamp) AS cumulative_earliest_cook_cycle_timestamp
FROM {{ table_reference('customer_term_cooks', 'wash') }}
GROUP BY 1,2 
