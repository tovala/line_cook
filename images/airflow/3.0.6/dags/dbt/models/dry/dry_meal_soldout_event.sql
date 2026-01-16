{{ dry_config('meal_soldout_event') }}

SELECT
  id
  , meal_id 
  , subterm_id 
  , sold_out_count
  , selections_count
  , created 
FROM {{ source('combined_api_v3', 'meal_soldout_event') }}

{{ load_incrementally(bookmark='created') }}
