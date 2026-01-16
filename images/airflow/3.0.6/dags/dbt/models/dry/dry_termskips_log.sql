{{ dry_config('termskips_log') }}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('action') }} AS action
  , auto_skip
  , created
  , TRY_PARSE_JSON(custom_fields) AS custom_fields
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('reason') }} AS reason
  , static_skip
  , termid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'termskips_log') }}

{{ load_incrementally() }}
