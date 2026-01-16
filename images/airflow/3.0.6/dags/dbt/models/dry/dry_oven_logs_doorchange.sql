{{ dry_config_oven_logs('oven_logs_doorchange') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:data:doorState::STRING') }} AS data_doorstate
  , {{ clean_string('raw_data:doorState::STRING') }} AS doorState
  , {{ clean_string('raw_data:environment::STRING') }} AS environment
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'doorChange'
{{ load_incrementally_oven_logs() }}