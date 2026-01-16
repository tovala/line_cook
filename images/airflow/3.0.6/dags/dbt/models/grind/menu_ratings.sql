
SELECT 
  DISTINCT menu_rating_id
  , customer_id
  , rating_time
  , menu_rating
  , source_os
  , term_id::INTEGER AS term_id
  , user_comment
FROM {{ table_reference('menu_feedback') }}