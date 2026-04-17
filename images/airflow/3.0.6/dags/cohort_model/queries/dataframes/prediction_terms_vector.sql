SELECT 
  'TERM_' || term_id AS term_id
FROM mugwort.future_terms 
WHERE term_id <= (SELECT MAX(term_id) FROM mugwort.combined_oven_sales);