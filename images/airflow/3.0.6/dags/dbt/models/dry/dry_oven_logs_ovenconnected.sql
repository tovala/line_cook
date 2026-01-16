{{ dry_config_oven_logs('oven_logs_ovenconnected') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:ipaddress::STRING') }} AS ipaddress
  , TRY_TO_BOOLEAN({{ clean_string('raw_data:isconnected::STRING') }}) AS isconnected
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid 
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'ovenConnected'
{{ load_incrementally_oven_logs() }}