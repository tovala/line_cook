{{ dry_config('usermealdatajson', primary_key='userid') }}

SELECT
  created
  , TRY_PARSE_JSON(data) AS data
  , updated
  , userid
FROM {{ source('combined_api_v3', 'usermealdatajson') }}

{{ load_incrementally() }}
