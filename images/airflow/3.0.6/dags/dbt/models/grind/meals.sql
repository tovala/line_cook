
SELECT 
  mns.meal_id
  , mns.internal_meal_name
  , mns.current_version_id
  , cmt.protein_category
  , cmt.protein_type
  , cmt.protein_subtype 
  , cmt.protein_format
  , cmt.cuisine_category
  , cmt.cuisine
  , cmt.dish_type
  , cmt.dish_format
  , cmt.side_category
  , cmt.side_type
  , TRY_TO_NUMERIC(cmt.health_score)::INTEGER AS health_score
  , TRY_TO_NUMERIC(cmt.adventurous_score)::INTEGER AS adventurous_score
  , TRY_TO_NUMERIC(cmt.complex_score)::INTEGER AS complex_score
  , MIN(mns.term_id) AS first_offered_term_id
  , MAX(mns.term_id) AS last_offered_term_id
  , COALESCE(MAX(t.term_id) - MAX(mns.term_id) < 55, FALSE) AS is_active_meal
  , COUNT(DISTINCT mns.term_id) AS terms_offered_count
  , COUNT(DISTINCT mns.version_id) AS versions_count
FROM {{ ref('meal_name_standardizations') }} mns
LEFT JOIN {{ table_reference('culinary_meal_tags') }} cmt 
  ON {{ clean_string('mns.internal_meal_name') }} = cmt.internal_meal_name
LEFT JOIN {{ ref('terms') }} t
  ON t.is_latest_completed
GROUP BY ALL
