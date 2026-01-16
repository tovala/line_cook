{{ dry_config('oven_roadshows') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , position_index
  , {{ clean_string('roadshow_location') }} AS roadshow_location
  , {{ clean_string('serial_number') }} AS serial_number
  , updated
FROM {{ source('combined_api_v3', 'oven_roadshows') }}

{{ load_incrementally() }}
