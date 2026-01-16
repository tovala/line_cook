
(SELECT 
   mo.term_id 
   , mo.customer_id
   , COALESCE(fus.wants_breakfast_autofill, FALSE) AS wants_breakfast_autofill
   , COALESCE(fus.wants_surcharged_autofill, FALSE) AS wants_surcharged_autofill
   , COALESCE(fus.wants_double_autofill, FALSE) AS wants_double_autofill
   , COALESCE(cf.eats_pork, FALSE) AS eats_pork
 FROM {{ ref('meal_orders') }} mo 
 LEFT JOIN {{ ref('full_user_statuses') }} fus
   ON  mo.customer_id = fus.customer_id
   AND mo.order_time >= fus.status_start_time
   AND mo.order_time < COALESCE(fus.status_end_time, '9999-12-31')
 LEFT JOIN {{ ref('customer_facts') }} cf 
   ON mo.customer_id = cf.customer_id)
UNION ALL 
-- Upcoming terms - haven't processed yet
(SELECT 
   fts.term_id 
   , fts.customer_id 
   , fts.wants_breakfast_autofill 
   , fts.wants_surcharged_autofill
   , fts.wants_double_autofill
   , fts.eats_pork
 FROM {{ ref('future_term_summary') }} fts
 INNER JOIN {{ ref('terms') }} te 
   ON fts.term_id = te.term_id 
   AND te.has_no_orders
 WHERE NOT fts.is_skipped 
   AND fts.latest_user_status = 'active')
