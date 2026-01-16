
SELECT
    RANK() OVER (PARTITION BY tl.userid, tl.termid, tl.action ORDER BY tl.created) AS row_rank
	, tl.id AS skip_click_id 
	, tl.userid AS customer_id 
	, tl.termid AS term_id 
	, tl.created AS click_time
	, tl.static_skip AS is_forced_skip
    , tl.action
	-- See note below for shitty edge case
	, COALESCE(CASE WHEN tl.action = 'INSERT' AND tl.termid = 179 AND tl.userid IN (117578, 80710) 
	                THEN DATEADD('second', 1, tl.created) 
			   END, 
		       roct.real_order_cutoff_time, t.order_by_time) AS skip_cutoff_time
FROM {{ table_reference('termskips_log') }} tl
LEFT JOIN {{ ref('terms') }} t 
	ON tl.termid = t.term_id 
LEFT JOIN {{ ref('real_order_cutoff_times') }} roct 
	ON tl.termid = roct.term_id
WHERE tl.termid >= 167 -- Note: This filter is not explicitly needed. We are including it in case anyone ever backfills the raw termskips_log table
AND tl.action IN ('INSERT', 'DELETE')
AND ( -- Filters to handle edge-cases with late skipping
	tl.created <= COALESCE(roct.real_order_cutoff_time, t.order_by_time) -- Note: We didn't always prevent people from skipping / unskipping on the UI after their order_by_time had elapsed; so we derive the actual cutoff time (time at which the last fulfilled order was placed) and compare against that; HOWEVER, sometimes, we don't have that time (for future terms), so we use the order_by_time from terms
	OR tl.termid = 179 -- Note: We asked customers to pause after-the-fact due to shipping shortages this term
	OR tl.notes IS NOT NULL -- Note: Sometimes, we update a status based on a ticket. Sometimes when we do that, we add the ticket to the notes. Therefore, if the notes aren't null, we can use that as a marker for "Something odd happened"
)