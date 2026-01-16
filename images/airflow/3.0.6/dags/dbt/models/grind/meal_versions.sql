
SELECT 
  version_id
  , current_version_id
  , version_id = current_version_id AS is_current_version
  , meal_id
  , internal_meal_name
  , MIN(term_id) AS first_offered_term_id
  , MAX(term_id) AS last_offered_term_id
  , COUNT(DISTINCT term_id) AS terms_offered_count
  , COUNT(DISTINCT version_id) AS versions_count
  , RANK() OVER (PARTITION BY meal_id ORDER BY first_offered_term_id) AS version_number
FROM {{ ref('meal_name_standardizations') }}
GROUP BY 1,2,3,4,5

--NOTE: the last version_number may not always be the is_current_version if a meal version reverts after another one was in production