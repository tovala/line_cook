{{ dry_config_oven_logs('oven_logs_cookingstarted') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode 
  , {{ clean_string('raw_data:cookCycleID::STRING') }} AS cookCycleID
  , {{ unique_cook_cycle_id() }}
  , raw_data:hwTimestamp::INTEGER AS hwTimestamp
  , {{ clean_string('raw_data:mode::STRING') }} AS mode
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , {{ clean_string('raw_data:ovenMode::STRING') }} AS ovenmode
  -- Typo from original data, correcting it here
  , raw_data:setDuaration::INTEGER AS setDuration
  , raw_data:setTemperature::INTEGER AS setTemperature 
  , {{ clean_string('raw_data:type::STRING') }} AS type
  -- routine is an array of JSON blogs of different cook steps
  , raw_data:routine AS routine
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'cookingStarted'
{{ load_incrementally_oven_logs() }}