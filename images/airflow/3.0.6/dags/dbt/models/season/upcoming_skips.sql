
SELECT 
  oss.skip_id 
  , oss.customer_id 
  , oss.term_id 
  , oss.skip_time 
  , oss.unskip_time
  , oss.skip_end_type AS status 
  , oss.is_forced_skip 
  , oss.is_break_skip
  , oss.meal_order_id
FROM {{ ref('ordinal_skip_statuses') }} oss
INNER JOIN {{ ref('terms') }} te
  ON oss.term_id = te.term_id
WHERE oss.row_number = 1 
  AND te.is_editable
