{{ dry_config_oven_logs('oven_logs_webcook') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode
  , {{ clean_string('raw_data:cookCycleID::STRING') }} AS cookCycleID
  , {{ unique_cook_cycle_id() }}
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid 
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'webCook'
{{ load_incrementally_oven_logs() }}
