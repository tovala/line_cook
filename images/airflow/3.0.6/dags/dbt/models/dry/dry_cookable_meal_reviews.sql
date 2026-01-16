{{ dry_config(
  table_name='cookable_meal_reviews',
  primary_key=None, 
  load_mode='table', 
  post_hooks=[]
)}} 


SELECT
  cookable_review_id 
  , created 
  , mealid 
  , routine_revision_id
FROM {{ source('combined_api_v3', 'cookable_meal_reviews') }}

{{ load_incrementally(bookmark='created') }}
