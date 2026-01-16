{{ dry_config('mealtags') }}

SELECT
  active_until
  , {{ clean_string('color') }} AS color
  , {{ clean_string('description') }} AS description
  , {{ clean_string('description_internal') }} AS description_internal
  , {{ clean_string('display_mode') }} AS display_mode
  , id
  , {{ clean_string('image') }} AS image
  , priority
  , {{ clean_string('title') }} AS title
  , updated
  , {{ clean_string('collapsed_title') }} AS collapsed_title
FROM {{ source('combined_api_v3', 'mealtags') }}

{{ load_incrementally() }}
