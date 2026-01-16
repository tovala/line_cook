
SELECT
  customer_id
  -- Generates one column per cook_event_type
  {{ dynamic_pivot_generator(source_pivot='cook_event_type', 
                             source_table='customer_oven_usage_totals', 
                             window_function='SUM',
                             pivot_column='cook_event_type', 
                             pivot_value_field='sum_total_completed_cooks', 
                             pivot_suffix='completed_cook_count') }}
  , MIN(cumulative_earliest_cook_cycle_timestamp)::DATE AS cook_cohort_date
  , MAX(cumulative_latest_cook_cycle_timestamp) AS latest_cook_time
  , latest_cook_time >= {{ current_timestamp_utc() }} - INTERVAL '4 weeks' AS is_active_oven_user
FROM {{ ref('customer_oven_usage_totals') }}
GROUP BY 1
