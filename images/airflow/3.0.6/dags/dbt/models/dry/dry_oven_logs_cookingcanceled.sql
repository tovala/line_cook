{{ dry_config_oven_logs('oven_logs_cookingcanceled') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode 
  , {{ clean_string('raw_data:cookCycleID::STRING') }} AS cookCycleID
  , {{ unique_cook_cycle_id() }}
  , raw_data:hwTimestamp::INTEGER AS hwTimestamp
  , {{ clean_string('raw_data:mode::STRING') }} AS mode
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , raw_data:timeRemaining::INTEGER AS timeRemaining
  , {{ clean_string('raw_data:type::STRING') }} AS type
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'cookingCanceled'
{{ load_incrementally_oven_logs() }}