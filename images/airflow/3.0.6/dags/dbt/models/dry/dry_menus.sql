{{ dry_config('menus') }}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('subterm_id') }} AS subterm_id
  , {{ clean_string('name') }} AS name
  , {{ clean_string('description') }} AS description
  , is_default
  , {{ clean_string('notes') }} AS notes
  , created
  , updated
FROM {{ source('combined_api_v3', 'menus') }}

{{ load_incrementally() }}