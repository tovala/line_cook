WITH overlapping_breaks_and_skips AS (
  SELECT 
    nb.skip_id
    , be.break_id
    , nb.customer_id 
    , nb.term_id 
    , nb.skip_time
    , nb.unskip_time 
    , be.break_time
    , be.unbreak_time
    -- NOTE: this will be NULL for the first overlap, so we will handle that separately
    , LAG(be.unbreak_time) OVER (PARTITION BY nb.customer_id, nb.term_id ORDER BY be.break_time) AS new_skip_time 
    , be.break_time AS new_unskip_time
    -- It is considered the first overlap if either it is the first overlapping break/skip for that customer and term OR if it the first time a particular skip overlaps with any break
    , ROW_NUMBER() OVER (PARTITION BY nb.customer_id, nb.term_id ORDER BY be.break_time) = 1 
        OR ROW_NUMBER() OVER (PARTITION BY nb.customer_id, nb.term_id, nb.skip_id ORDER BY be.break_time) = 1 AS is_first_overlap
    , ROW_NUMBER() OVER (PARTITION BY nb.customer_id, nb.term_id ORDER BY be.break_time DESC) = 1 
        OR ROW_NUMBER() OVER (PARTITION BY nb.customer_id, nb.term_id, nb.skip_id ORDER BY be.break_time DESC) = 1 AS is_last_overlap
    , nb.is_forced_skip
    , nb.skip_end_type
  FROM {{ ref('non_break_skip_events') }} AS nb
  INNER JOIN {{ ref('break_skip_events') }} AS be 
    ON nb.term_id = be.term_id 
    AND nb.customer_id = be.customer_id
    AND (
      -- Case 1: A user who is skipped goes on a break. We verify that the skip does not end before the break starts. 
      (nb.skip_time < be.break_time AND be.break_time < COALESCE(nb.unskip_time, '9999-12-31')) OR      
      -- Case 2: A user who is on a break is force skipped
      (be.break_time < nb.skip_time AND nb.skip_time < COALESCE(be.unbreak_time, '9999-12-31')))
)
-- 1. Include all skips that don't have a break overlapping with them 
(SELECT 
   skip_id 
   , NULL AS break_id
   , customer_id
   , term_id 
   , skip_time
   , unskip_time
   , skip_end_type
   , is_forced_skip
   , FALSE AS is_break_skip
   , 'original_skip' AS overlap_type
 FROM {{ ref('non_break_skip_events') }} 
 WHERE skip_id NOT IN (SELECT skip_id FROM overlapping_breaks_and_skips))
UNION ALL 
-- 2. Include all breaks 
(SELECT 
   skip_id 
   , break_id
   , customer_id
   , term_id 
   , break_time AS skip_time
   , unbreak_time AS unskip_time
   , skip_end_type
   , is_forced_skip
   , TRUE AS is_break_skip
   , 'original_break' AS overlap_type
 FROM {{ ref('break_skip_events') }})
UNION ALL 
-- 3. Generate new skips that span from the last state change to the start of the overlapping break 
(SELECT 
   {{ hash_natural_key('skip_id', 'break_id', 'new_skip_time')}} AS skip_id 
   , break_id 
   , customer_id
   , term_id 
   , new_skip_time AS skip_time
   , new_unskip_time AS unskip_time
   , 'unskipped' AS skip_end_type 
   , is_forced_skip
   , FALSE AS is_break_skip
   , 'mid_overlap' AS overlap_type
 FROM overlapping_breaks_and_skips
 WHERE NOT is_first_overlap)
UNION ALL 
-- 4. Handle a final skip that occurs if the last unbreak time is not null and there is a skip that exists past the end of that break 
(SELECT 
   {{ hash_natural_key('skip_id', 'break_id', 'unbreak_time')}} AS skip_id
   , break_id 
   , customer_id 
   , term_id 
   , unbreak_time AS skip_time 
   -- Both skip_end_type and unskip_time remain the same as the original skip b/c it terminates in the same way
   , unskip_time 
   , skip_end_type 
   , is_forced_skip
   , FALSE AS is_break_skip
   , 'end_overlap' AS overlap_type
 FROM overlapping_breaks_and_skips
 -- If it isn't the last overlap, there is definitionally another overlap before it
 WHERE is_last_overlap
   -- If unbreak_time is NULL, the break is ongoing and so no skip occurs afterwards
   AND unbreak_time IS NOT NULL
   -- If the unbreak_time is BEFORE the unskip_time (regardless of whether or not it is NULL), there is a dangling skip
   AND COALESCE(unbreak_time, '9999-12-31') < COALESCE(unskip_time, '9999-12-31'))
UNION ALL
-- 5. Handle the first overlap 
(SELECT 
   {{ hash_natural_key('skip_id', 'break_id', 'break_time')}} AS skip_id
   , break_id 
   , customer_id 
   , term_id 
   -- The skip_time remains the same because it is BEFORE the first break
   , skip_time 
   , break_time AS unskip_time 
   , 'unskipped' AS skip_end_type 
   , is_forced_skip
   , FALSE AS is_break_skip
   , 'start_overlap' AS overlap_type
 FROM overlapping_breaks_and_skips
 -- If this is the first overlap and the break starts after the skip does
 WHERE is_first_overlap
   AND skip_time < break_time)
