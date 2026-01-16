{{ dry_config_oven_logs('oven_logs_fellbacktobarcodebackup') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:barcode::STRING') }} AS barcode
  , {{ clean_string('TRY_PARSE_JSON(raw_data:body):"message"::STRING') }} AS body_message
  , {{ clean_string('TRY_PARSE_JSON(raw_data:body):"error"::STRING') }} AS body_error
  , {{ clean_string('raw_data:body::STRING') }} AS body_raw
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , {{ clean_string('raw_data:statusCode::STRING') }} AS statusCode
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'fellBackToBarcodeBackup'
{{ load_incrementally_oven_logs() }}