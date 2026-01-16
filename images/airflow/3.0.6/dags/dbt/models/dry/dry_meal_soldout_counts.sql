{{ dry_config('meal_soldout_counts') }}

SELECT
  created
  , id
  , is_sold_out
  , last_sold_out_time
  , mealid
  , {{ clean_string('notes') }} AS notes
  , soldout_count
  , subterm_id
  , updated
FROM {{ source('combined_api_v3', 'meal_soldout_counts') }}

{{ load_incrementally() }}