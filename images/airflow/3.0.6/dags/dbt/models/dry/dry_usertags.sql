{{ dry_config('usertags') }}

SELECT
  TRY_PARSE_JSON(data) AS data
  , {{ clean_string('id') }} AS id
  , {{ clean_string('tag') }} AS tag
  , updated
  , userid
FROM {{ source('combined_api_v3', 'usertags') }}

{{ load_incrementally() }}
