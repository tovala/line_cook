{{ dry_config_oven_logs('oven_logs_apirequest') }}

SELECT
  {{ oven_logs_base(True) }}
  , raw_data:body AS body -- TO DO: parse
  , raw_data:headers AS headers -- TO DO: parse
  , {{ clean_string('raw_data:method::STRING') }} AS method
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , {{ clean_string('raw_data:path::STRING') }} AS path
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'apiRequest'
{{ load_incrementally_oven_logs() }}
