{{ dry_config_oven_logs('oven_logs_wificonnected') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid 
  , raw_data:ipv4 AS ipv4 -- NOTE: the ip address in this is incorrect 
  , raw_data:interface AS interface 
  , {{ clean_string('interface[0]:bssid::STRING') }} AS bssid 
  , AS_INTEGER(interface[0]:rssi) AS rssi
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'WifiConnected'
{{ load_incrementally_oven_logs() }}