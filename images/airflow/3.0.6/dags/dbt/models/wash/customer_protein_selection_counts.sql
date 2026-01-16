
SELECT DISTINCT
  c.customer_id 
  , SUM(CASE WHEN msk.contains_pork AND NOT msk.contains_bacon_bits THEN 1 ELSE 0 END) AS pork_selection_count
  , SUM(CASE WHEN msk.contains_tofu THEN 1 ELSE 0 END) AS tofu_selection_count
FROM {{ ref('customers') }} c 
LEFT JOIN {{ ref('meal_selections') }} ms 
  ON c.customer_id = ms.customer_id
  AND ms.is_fulfilled 
  AND NOT ms.is_autoselection 
  AND ms.term_id < {{ live_term() }}
LEFT JOIN {{ ref('meal_skus') }} msk 
  ON ms.meal_sku_id = msk.meal_sku_id 
GROUP BY 1 
