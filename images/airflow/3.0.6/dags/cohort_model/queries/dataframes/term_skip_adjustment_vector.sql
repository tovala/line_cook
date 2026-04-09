WITH skip_adj AS (
  SELECT DISTINCT term_id, holiday_skip_adjustment 
  FROM {{ params.database}}.mugwort.skip_adjustments 
  WHERE term_id BETWEEN 
    (SELECT term_id FROM {{ params.database }}.grind.terms WHERE is_live) -- cohort model prediction starting term (current live term)
    AND 
    (SELECT MAX(term_id) FROM {{ params.database }}.mugwort.combined_oven_sales) -- cohort model prediction ending term (last term with predicted oven sales)
  ORDER BY term_id
)
SELECT holiday_skip_adjustment from skip_adj; -- just the ordered skip adj values in vector form for the relevant terms