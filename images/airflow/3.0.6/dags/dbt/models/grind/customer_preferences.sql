
SELECT 
  bpr.customer_id
  , (bpr.chicken_ranking = 1 AND bpr.beef_ranking = 1 AND bpr.pork_ranking = 1 AND bpr.fish_ranking = 1 AND bpr.shellfish_ranking = 1) AS is_likely_vegetarian
  , (is_likely_vegetarian AND bpr.eggs_ranking = 1 AND bpr.dairy_ranking = 1) AS is_likely_vegan
  , (bpr.chicken_ranking = 1 AND bpr.beef_ranking = 1 AND bpr.pork_ranking = 1 AND bpr.fish_ranking IN (4,5) AND bpr.shellfish_ranking IN (4,5)) AS is_likely_pescatarian
  , (bpr.dairy_ranking = 1) AS is_likely_dairy_intolerant
  , (bpr.gluten_ranking = 1) AS is_likely_gluten_intolerant
  , (bpr.spicy_foods_ranking = 1) AS is_likely_spicy_foods_intolerant
  , (bpr.tree_nuts_ranking = 1) AS is_likely_tree_nuts_intolerant
FROM {{ ref('big_picture_responses') }} bpr
WHERE bpr.nth = 1
