{{ dry_config_oven_logs('oven_logs_errorcode') }}

SELECT
  {{ oven_logs_base(True) }}
  , raw_data:class::INTEGER AS class
  , raw_data:error::INTEGER AS error
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'errorCode'
{{ load_incrementally_oven_logs() }}
