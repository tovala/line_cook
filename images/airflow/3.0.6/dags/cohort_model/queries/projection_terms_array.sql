SELECT 
  'TERM_' || term_id AS term_id
FROM mugwort.future_terms 
WHERE term_id BETWEEN {{ get_projection_start_term.output }} AND {{ get_projection_end_term.output }}
AND NOT is_skipped_term
ORDER BY term_id;