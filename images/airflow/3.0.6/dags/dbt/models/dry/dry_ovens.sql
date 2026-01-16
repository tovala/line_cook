{{ dry_config('ovens') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('model') }} AS model
  , {{ clean_string('name') }} AS name
  , {{ clean_string('type') }} AS type
  , updated
  , userid
FROM {{ source('combined_api_v3', 'ovens') }}

{{ load_incrementally() }}
