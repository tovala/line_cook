{{ dry_config('ovens_tovala') }}

SELECT
  {{ clean_string('agentid') }} AS agentid
  , created
  , {{ clean_string('deviceid') }} AS deviceid
  , {{ clean_string('id') }} AS id
  , {{ clean_string('planid') }} AS planid
  , {{ clean_string('serial') }} AS serial
  , updated
FROM {{ source('combined_api_v3', 'ovens_tovala') }}

{{ load_incrementally() }}
