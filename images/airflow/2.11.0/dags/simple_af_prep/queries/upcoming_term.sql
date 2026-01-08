SELECT 
  MIN(term_id) AS term_id
FROM grind.terms 
WHERE is_editable;