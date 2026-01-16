{{ dry_config_oven_logs('oven_logs_cookingended') }}

SELECT
{{ oven_logs_base() }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode 
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid 
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'cookingEnded'
{{ load_incrementally_oven_logs() }}