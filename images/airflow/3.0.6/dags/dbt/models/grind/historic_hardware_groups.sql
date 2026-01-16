{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key=['oven_hardware_id', 'group_start_date']
    ) 
}}

/*

kinesis contents on 10/25/2024

deviceA, group1, 10/10/24
deviceA, group2, 10/11/24
deviceB, group1, 10/10/24
deviceB, group2, 10/11/24
deviceB, group2, 10/12/24
deviceB, group2, 10/13/24
deviceB, group3, 10/25/24
deviceC, group1, 10/10/24

historic_hardware_groups contents on 10/25/2024, before first ETL run

deviceA, group1, 10/10/24, 10/11/24, false
deviceA, group2, 10/11/24, null    , true
deviceB, group1, 10/10/24, 10/13/24, false
deviceB, group2, 10/13/24, null    , true
deviceC, group1, 10/10/24, null    , true

*/

/*

This gets our base table, with a row for each device on each day it had an event logged, and the hardware group it was in on the last event of the day

deviceA, group1, 10/10/24
deviceA, group2, 10/11/24
deviceB, group1, 10/10/24
deviceB, group2, 10/11/24
deviceB, group2, 10/12/24
deviceB, group2, 10/13/24
deviceB, group3, 10/25/24
deviceC, group1, 10/10/24

Incrementally, it gets a row for each device on each day since the last run, inferred by the last group_end_date

deviceB, group3, 10/25/24

*/
WITH daily_device_states AS (
  SELECT 
    deviceid as oven_hardware_id
    , to_date(time_stamp) AS group_date
    -- Get the a group for each device on each day
    , MAX_BY(devicegroupname, time_stamp) AS hardware_group_name
  FROM {{ source('kinesis', 'oven_logs_combined') }}
  WHERE devicegroupname IS NOT NULL
  {% if is_incremental() %}
    AND to_date(time_stamp) >= (SELECT MAX(group_end_date) FROM {{ this }})
  {% endif %}
  GROUP BY 1, 2
),

/* because the incremental run only has data for the days since last run, we need to get previous states to add on. Only current states are relevant here:

deviceA, group2, 10/11/24
deviceB, group2, 10/13/24
deviceC, group1, 10/10/24

*/
{% if is_incremental() %}
  previous_states AS (
    SELECT 
      oven_hardware_id
    , hardware_group_name
    , group_start_date AS group_date
    FROM {{ this }}
    WHERE is_current = TRUE
  ),
{% endif %}

/* we union on the previous states to the new states from the incremental run

deviceA, group2, 10/11/24
deviceB, group2, 10/13/24
deviceC, group1, 10/10/24
deviceB, group3, 10/25/24

now the incremental and full-refresh tables are (functionally) the same, and we can perform the same operation on them

*/
combined_states AS (
  {% if is_incremental() %}
    SELECT oven_hardware_id
           , group_date
           , hardware_group_name 
    FROM daily_device_states
    UNION ALL
    SELECT oven_hardware_id
           , group_date
           , hardware_group_name 
    FROM previous_states
  {% else %}
    SELECT oven_hardware_id, group_date, hardware_group_name FROM daily_device_states
  {% endif %}
),

/*

Look at what group the device was in on the previous row


incremental:

deviceA, group2, 10/11/24, null
deviceB, group2, 10/11/24, null
deviceC, group1, 10/10/24, null
deviceB, group3, 10/25/24, group2

full-refresh:

deviceA, group1, 10/10/24, null
deviceA, group2, 10/11/24, group1
deviceB, group1, 10/10/24, null
deviceB, group2, 10/11/24, group1
deviceB, group2, 10/12/24, group2
deviceB, group2, 10/13/24, group2
deviceB, group3, 10/25/24, group2
deviceC, group1, 10/10/24, null

*/

group_changes AS (
  SELECT
    oven_hardware_id
    , hardware_group_name
    , group_date AS group_start_date
    -- Look at what group this device was in the day before
    , LAG(hardware_group_name) OVER (
        PARTITION BY oven_hardware_id
        ORDER BY group_date
      ) AS previous_group
  FROM combined_states
)

/*

filter out first rows and rows where the previous group is the same as the current group, then assign the next row's date as their end date

incremental:

deviceA, group2, 10/11/24, null
deviceB, group2, 10/11/24, 10/25/24
deviceC, group1, 10/10/24, null
deviceB, group3, 10/25/24, null

full-refresh:

deviceA, group1, 10/10/24, 10/11/24
deviceA, group2, 10/11/24, null
deviceB, group1, 10/10/24, 10/11/24
deviceB, group2, 10/11/24, 10/25/24
deviceB, group3, 10/25/24, null
deviceC, group1, 10/10/24, null

*/

-- Only keep the records where the group changed
SELECT
  oven_hardware_id
  , hardware_group_name
  , group_start_date
-- The next change for this device becomes the end date for this record
  , LEAD(group_start_date) OVER (
      PARTITION BY oven_hardware_id
      ORDER BY group_start_date
    ) AS group_end_date
  , group_end_date IS NULL AS is_current
FROM group_changes
WHERE previous_group IS NULL -- First time we've seen this device
   OR previous_group != hardware_group_name -- Group changed

/*from those final tables, the incremental run will update deviceB start_date 10/11/24 to have an end date 10/25/24, 
and add a row for deviceB, group3, 10/25/24, null. DeviceA and DeviceC will be overwritten but not changed
*/