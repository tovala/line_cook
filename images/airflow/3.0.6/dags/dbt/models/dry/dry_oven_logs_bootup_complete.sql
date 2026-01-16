{{ dry_config_oven_logs('oven_logs_bootup_complete') }}

SELECT
  {{ oven_logs_base(True) }}
  , {{ clean_string('raw_data:currentState::STRING') }} AS currentState
  , COALESCE({{ clean_string('raw_data:firmwareVerion::STRING') }} , {{ clean_string('raw_data:firmwareVersion::STRING') }} ) AS firmwareVerion
  , raw_data:freeMemory::FLOAT AS freeMemory
  , {{ clean_string('raw_data:ovenid::STRING') }} AS ovenid
  , {{ clean_string('raw_data:reason::STRING') }} AS reason
  , {{ clean_string('raw_data:wakeReason::STRING') }} AS wakeReason
FROM {{ source('kinesis', 'oven_logs_combined') }}
WHERE key = 'bootup-complete'
{{ load_incrementally_oven_logs() }}