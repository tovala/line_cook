{{ dry_config_oven_logs('oven_logs_barcodescanned') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'barcodeScanned'
{{ load_incrementally_oven_logs() }}