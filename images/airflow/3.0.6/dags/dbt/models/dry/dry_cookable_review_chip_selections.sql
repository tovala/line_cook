{{ dry_config(
  table_name='cookable_review_chip_selections', 
  primary_key=None, 
  load_mode='table', 
  post_hooks=[]
)}}
-- TODO: Update the config once a timestamp field exists

SELECT
  cookable_review_chip_id 
  , cookable_review_id 
  , created_at
FROM {{ source('combined_api_v3', 'cookable_review_chip_selections') }}

-- TODO: Load incrementally once a timestamp field exists (https://tovala.atlassian.net/browse/DAT-3213)
