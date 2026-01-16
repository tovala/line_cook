
SELECT  
  MAX_BY(eva.variant_id, eva.variant_seen_time) AS variant_id 
  , eva.customer_id 
  , mv.term_id
FROM {{ ref('experiments_variants_assignments') }} eva
LEFT JOIN {{ ref('menu_variants') }} mv
  ON eva.variant_id = mv.variant_id
GROUP BY eva.customer_id, mv.term_id
