{{ dry_config_oven_logs('oven_logs_global_exception') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:error::STRING') }} AS error
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'global_exception'
{{ load_incrementally_oven_logs() }}