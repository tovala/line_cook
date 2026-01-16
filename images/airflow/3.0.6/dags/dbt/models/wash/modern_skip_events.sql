
WITH raw_skip_events AS (
  SELECT
    sc.skip_click_id
    , sc.customer_id 
    , sc.term_id
    , sc.click_time AS raw_skip_time 
    , usc.click_time AS raw_unskip_time 
    , sc.is_forced_skip
    -- TODO: get rid of skip cutoff time ffs 
    , mo.status = 'skipped' AND NOT mo.is_break_skip AS is_final_skipped
    -- We need to have an end time for the skip so we can see where it overlaps with order statuses. 
      -- 1. If there is an actual unskip, use that
      -- 2. Next, if there is a meal order time use that. 
      -- 3. If for some unholy reason there is neither, use the last order processed time 
        -- For unprocessed orders, this is the term close time 
    , COALESCE(usc.click_time, mo.order_time, sc.skip_cutoff_time) AS skip_end_time
  FROM {{ ref('skip_unskip_clicks') }} sc 
  LEFT JOIN {{ ref('skip_unskip_clicks') }} usc 
    ON sc.customer_id = usc.customer_id 
    AND sc.term_id = usc.term_id 
    AND sc.row_rank = usc.row_rank
    AND sc.term_id >= 167
    AND usc.action = 'DELETE'
  LEFT JOIN {{ ref('meal_orders') }} mo 
    ON mo.customer_id = sc.customer_id 
    AND mo.term_id = sc.term_id 
  WHERE sc.action = 'INSERT')
SELECT 
  se.skip_click_id
  , uas.status_id 
  , se.customer_id 
  , se.term_id 
  , CASE WHEN uas.status_start_time < se.raw_skip_time 
              AND se.skip_end_time < COALESCE(uas.status_end_time, '9999-12-31') 
         THEN 'case_1'
         WHEN se.raw_skip_time < uas.status_start_time 
              AND COALESCE(uas.status_end_time, '9999-12-31') < se.skip_end_time 
         THEN 'case_2'
         WHEN uas.status_start_time < se.raw_skip_time 
              AND se.raw_skip_time < COALESCE(uas.status_end_time, '9999-12-31') 
              AND COALESCE(uas.status_end_time, '9999-12-31') < se.skip_end_time 
         THEN 'case_3'
         WHEN se.raw_skip_time < uas.status_start_time
              AND uas.status_start_time < se.skip_end_time
              AND se.skip_end_time < COALESCE(uas.status_end_time, '9999-12-31') 
         THEN 'case_4'
    END AS overlap_category
  , CASE WHEN overlap_category IN ('case_1', 'case_3')
         THEN se.raw_skip_time 
         WHEN overlap_category IN ('case_2', 'case_4')
         THEN uas.status_start_time 
    END AS skip_time
  , CASE WHEN overlap_category IN ('case_1', 'case_4')
         THEN se.raw_unskip_time 
         WHEN overlap_category IN ('case_2', 'case_3')
         THEN uas.status_end_time 
    END AS adjusted_unskip_time
  , se.is_forced_skip
  -- included for testing/debugging purposes 
  , se.skip_end_time
  , uas.status_start_time
  , uas.status_end_time
  , se.raw_skip_time AS original_skip_time
  , se.raw_unskip_time AS original_unskip_time 
  -- Ensure that if we have a record customer who should be skipped, they show up as skipped 
  , CASE WHEN ROW_NUMBER() OVER (PARTITION BY se.customer_id, se.term_id ORDER BY skip_time DESC) = 1 -- Most recent record
              AND COALESCE(se.is_final_skipped, FALSE) AND adjusted_unskip_time IS NOT NULL -- but also has skipped meal order and unskip isn't null
         THEN NULL 
         ELSE adjusted_unskip_time
    END AS unskip_time
FROM raw_skip_events se
INNER JOIN {{ ref('user_activity_statuses') }} uas 
  ON se.customer_id = uas.customer_id 
  AND uas.user_status = 'active'
  AND {{ date_overlap('se.raw_skip_time', 'se.skip_end_time', 'uas.status_start_time', 'COALESCE(uas.status_end_time, \'9999-12-31\')') }}
