{{ dry_config('terms') }}

SELECT
  end_date
  , id
  , order_by_date
  , ready_for_view
  , {{ clean_string('special_event') }} AS special_event
  , start_date
  , updated
FROM {{ source('combined_api_v3', 'terms') }}

{{ load_incrementally() }}
