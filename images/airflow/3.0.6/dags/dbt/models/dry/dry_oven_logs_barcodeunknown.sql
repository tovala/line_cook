{{ dry_config_oven_logs('oven_logs_barcodeunknown') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'barcodeUnknown'
{{ load_incrementally_oven_logs() }}