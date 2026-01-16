{{ dry_config_oven_logs('oven_logs_temperature') }}

/*
Temp data comes through in two different ways depending on source:
    For source = mini/airvala/gen2, there are multiple un-nested keys (ex: chamberTemperature)
    For source = oven, temperature comes through as a blob with 5 values: ["boilerNTC","chamber","doorNTC","relayNTC","wall"]
*/
SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:cookCycleID::STRING') }}AS cookCycleID
  , {{ unique_cook_cycle_id() }}
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , raw_data:chamberTemperature::INT AS chamberTemperature
  , raw_data:wallTemperature::INT AS wallTemperature
  , raw_data:barrelTemperature::INT AS barrelTemperature
  , raw_data:temperature:chamber::FLOAT AS temperature_chamber
  , raw_data:temperature:wall::FLOAT AS temperature_wall
  , raw_data:temperature:boilerNTC::FLOAT AS temperature_boilerNTC
  , raw_data:temperature:doorNTC::FLOAT AS temperature_doorNTC
  , raw_data:temperature:relayNTC::FLOAT AS temperature_relayNTC
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'temperature'
{{ load_incrementally_oven_logs() }}