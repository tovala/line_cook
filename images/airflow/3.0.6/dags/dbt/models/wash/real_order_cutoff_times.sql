
SELECT
  mo.term_id 
  , GREATEST(t.order_by_time, MAX(mo.order_time)) AS real_order_cutoff_time 
FROM {{ ref('meal_orders') }} mo
INNER JOIN {{ ref('terms') }} t 
  ON mo.term_id = t.term_id 
WHERE mo.is_fulfilled 
GROUP BY mo.term_id, t.order_by_time
