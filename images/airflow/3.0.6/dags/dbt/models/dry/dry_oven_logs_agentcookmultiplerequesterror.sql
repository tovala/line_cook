{{ dry_config_oven_logs('oven_logs_agentcookmultiplerequesterror') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'agentCookMultipleRequestError'
{{ load_incrementally_oven_logs() }}