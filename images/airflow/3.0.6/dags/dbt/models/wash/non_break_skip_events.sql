
-- Historical Skips (pre term 167)
(SELECT
  {{ hash_natural_key('userid', 'termid', 'created', 'updated') }} AS skip_id 
   , userid AS customer_id 
   , termid AS term_id 
   , created AS skip_time
   , NULL::TIMESTAMP_TZ AS unskip_time 
   , 'skipped' AS skip_end_type
   , static_skip AS is_forced_skip
  FROM {{ table_reference('termskips') }} 
  WHERE termid < 167)
 UNION ALL 
 (SELECT
   {{ hash_natural_key('skip_click_id', 'status_id') }} AS skip_id
   , customer_id 
   , term_id 
   , skip_time
   , unskip_time 
   , CASE WHEN unskip_time IS NULL 
          THEN 'skipped'
          -- Original skip end time kept
          WHEN overlap_category IN ('case_4', 'case_1')
          THEN 'unskipped'
          ELSE 'account_inactive'
     END AS skip_end_type
   , is_forced_skip
 FROM {{ ref('modern_skip_events') }})
UNION ALL 
-- Add skips for people who should have one but don't - can happen if we update meal order status but don't add a termskip
(SELECT 
   {{ hash_natural_key('customer_id', 'term_id', 'order_time', 'order_time') }} AS skip_id 
   , customer_id 
   , term_id 
   , order_time AS skip_time 
   , NULL::TIMESTAMP AS unskip_time
   , 'skipped' AS skip_end_type
   , FALSE AS is_forced_skip -- Note: This is not necessarily true. 
 FROM {{ ref('meal_orders') }} mo 
 WHERE status = 'skipped' AND NOT is_break_skip
 -- Don't count rows where a skip occurred after the meal_order status
 AND NOT EXISTS ( -- for Terms on/after 167
   SELECT 1 
   FROM {{ ref('modern_skip_events') }}
   WHERE customer_id = mo.customer_id 
   AND term_id = mo.term_id
   -- i.e. There is no skip that 'counted'
   AND unskip_time IS NULL
 )
AND NOT EXISTS ( -- for Terms before 167
   SELECT 1 
   FROM {{ table_reference('termskips') }}
   WHERE userid = mo.customer_id 
   AND termid = mo.term_id 
   AND termid < 167
))
