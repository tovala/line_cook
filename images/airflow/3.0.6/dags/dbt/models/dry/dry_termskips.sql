{{ dry_config('termskips', primary_key = 'termskip_id', post_hooks=[]) }}

SELECT
  {{ hash_natural_key('userid', 'termid', 'created') }} AS termskip_id
  , auto_skip
  , created
  , {{ clean_string('reason') }} AS reason
  , static_skip
  , termid
  , updated
  , userid
FROM {{ source('combined_api_v3', 'termskips') }}

{{ load_incrementally() }}
