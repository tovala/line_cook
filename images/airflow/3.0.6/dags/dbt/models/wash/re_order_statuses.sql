-- This table will fetch data for each customer who placed a non-autoselection fulfilled meal in a term
-- All the listing customers: has purchased the listing meal's last time it was offered
-- And a indicator for "is the customer re-order the listing meal"
-- To calculate the re-order rate downstream: for each listing meal COUNT(CASE WHEN is_reorder = TRUE THEN 1 END)/COUNT(customer_id) 

SELECT
  {{ hash_natural_key('current_term.customer_id', 'mms.menu_meal_id') }} AS customer_menu_meal_id 
  , mms.menu_meal_id 
  , mms.meal_sku_id
  , current_term.customer_id AS customer_id
  , current_term.term_id AS term_id
  , MAX(IFF( current_term.meal_sku_id = mms.meal_sku_id, TRUE, FALSE)) AS is_reorder
FROM {{ ref('combined_meal_selections') }} previous_term
INNER JOIN {{ ref('menu_meal_summary') }} mms
  ON previous_term.menu_meal_id = mms.prior_offered_menu_meal_id
  AND mms.prior_offered_menu_meal_id IS NOT NULL
INNER JOIN {{ ref('combined_meal_selections') }} current_term
  ON mms.term_id = current_term.term_id
  AND previous_term.customer_id = current_term.customer_id
GROUP BY 1,2,3,4,5
