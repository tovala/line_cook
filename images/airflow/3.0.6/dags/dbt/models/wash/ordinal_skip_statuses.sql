
SELECT 
  ROW_NUMBER() OVER (
        PARTITION BY s.customer_id, s.term_id
        ORDER BY s.skip_time DESC)
  AS row_number 
  , s.skip_id
  , s.customer_id 
  , s.term_id 
  , s.skip_time 
  , s.unskip_time 
  , s.skip_end_type
  , s.is_forced_skip 
  , s.is_break_skip
  , mo.meal_order_id 
FROM {{ ref('combined_skip_events') }} s
LEFT JOIN {{ ref('meal_orders') }} mo 
  ON s.customer_id = mo.customer_id 
  AND s.term_id = mo.term_id 
