{{ dry_config_oven_logs('oven_logs_huge_allocation') }}

SELECT
  {{ oven_logs_base(True) }}
  , raw_data:allocated::FLOAT AS allocated
  , {{ clean_string('raw_data:currentState::STRING') }} AS currentstate
  , raw_data:freeMemory::FLOAT AS freeMemory
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'huge_allocation'
{{ load_incrementally_oven_logs() }}