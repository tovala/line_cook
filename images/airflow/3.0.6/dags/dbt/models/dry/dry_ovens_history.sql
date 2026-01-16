{{ dry_config('ovens_history', primary_key = 'ovens_history_id', post_hooks=[]) }}

SELECT
  {{ hash_natural_key('ovenid', 'created') }} AS ovens_history_id
  , {{ clean_string('action') }} AS action
  , created
  , TRY_PARSE_JSON(custom_fields) AS custom_fields_json
  , {{ clean_string('custom_fields_json:"agentid"::STRING') }} AS agent_id
  , {{ clean_string('custom_fields_json:"deviceid"::STRING') }} AS device_id
  , {{ clean_string('custom_fields_json:"planid"::STRING') }} AS plan_id
  , {{ clean_string('custom_fields_json:"serial"::STRING') }} AS serial_number
  , {{ clean_string('model') }} AS model
  , {{ clean_string('name') }} AS name
  , {{ clean_string('ovenid') }} AS ovenid
  , {{ clean_string('type') }} AS type
  , updated
  , userid
FROM {{ source('combined_api_v3', 'ovens_history') }}

{{ load_incrementally() }}
