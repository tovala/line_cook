SELECT
  agentid AS agent_id
  , created
  , deviceid AS device_id
  , id
  , planid AS plan_id
  , serial AS serial_number
  , updated
FROM {{ table_reference('ovens_tovala') }}
-- Due to lag in combined_api, there are sometimes duplicate records for a given device_id that shouldn't exist
QUALIFY ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY updated DESC) = 1
