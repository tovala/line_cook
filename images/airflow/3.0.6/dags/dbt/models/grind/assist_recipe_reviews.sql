
-- New review format
(SELECT 
   rr.cookable_review_id AS review_id
   , rr.tovala_assist_recipe_id 
   , rr.created AS review_time
   , cr.userid AS customer_id 
   , cr.rating_int AS rating
   , cr.comment AS rating_comment
   , cr.platform 
 FROM {{ table_reference('cookable_assist_recipe_reviews') }} rr  
 LEFT JOIN {{ table_reference('cookable_reviews') }} cr 
   ON rr.cookable_review_id = cr.id
 INNER JOIN {{ ref('leads') }} l
   ON l.lead_id = cr.userid)
UNION
(SELECT 
   ta.id AS review_id
   , ta.recipe_id AS tovala_assist_recipe_id
   , ta.created AS review_time
   , ta.userid AS customer_id 
   -- Normalizes the rating so it is the same as the new recipe reviews
   , CASE WHEN ta.rating = 0
          THEN 50 
          WHEN ta.rating = -1 
          THEN 0
          WHEN ta.rating = 1
          THEN 100
     END AS rating 
   , ta.comment AS rating_comment
   , ta.medium AS platform 
 FROM {{ table_reference('tovala_assist_recipe_reviews') }} ta
 INNER JOIN {{ ref('leads') }} l
   ON l.lead_id = ta.userid
 WHERE valid)
