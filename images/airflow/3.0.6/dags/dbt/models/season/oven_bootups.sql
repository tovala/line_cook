{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='oven_event_id'
    ) 
}}

WITH 
  {% if is_incremental() %}
  incremental_data_source AS (
    SELECT 
      DISTINCT oven_hardware_id
    FROM {{ table_reference('oven_events', 'grind') }}
    WHERE key = 'bootup-complete'
      AND updated >= (SELECT MAX(updated) FROM {{ this }})
  ),
  {% endif %}
  base_source AS (
    SELECT 
      oe.oven_event_id
      , oe.agent_id
      , oe.hardware_group_id
      , oe.hardware_group_name
      , oe.oven_hardware_id
      , oe.session_id
      , oe.event_time AS bootup_time
      , oe.local_event_time AS local_bootup_time
      , oe.serial_number
      , oe.customer_id
      , oe.oven_registration_id
      , oe.oven_generation
      , oe.key
      , oe.batch_number
      , oe.current_hardware_group
      , oe.is_internal_account
      , oe.is_test_oven
      , oe.updated
    FROM {{ table_reference('oven_events', 'grind') }} oe
    WHERE oe.key = 'bootup-complete'
    {% if is_incremental() %}
      AND oe.oven_hardware_id IN (SELECT oven_hardware_id FROM incremental_data_source)
    {% endif %}
  ), tco_cook_events AS (
    SELECT DISTINCT 
      bc.oven_event_id 
    FROM {{ ref('cook_events') }} cec
    INNER JOIN {{ table_reference('oven_logs_bootup_complete') }} bc 
      ON cec.oven_hardware_id = bc.deviceid
      AND cec.cook_start_time <= bc.time_stamp 
      -- 10 seconds accounts for the difference between hardware time and measured time
      AND TIMESTAMPADD('seconds', 10, cec.expected_end_time) > bc.time_stamp
    -- TCO Rules (gen2/airvala only):
    -- 1. Cook event has a start but no end or cancel.
    -- 2. Bootup reason is power_on or power_restored
    -- 3. Bootup occurs before the expected end of the cook event (limits to only steady state cook events)  
    WHERE cec.cook_end_time IS NULL
      AND cec.expected_end_time IS NOT NULL
    {% if is_incremental() %}
      AND bc.deviceid IN (SELECT oven_hardware_id FROM incremental_data_source) 
    {% endif %}
  )
SELECT
  bs.oven_event_id
  , bs.agent_id
  , bs.hardware_group_id
  , bs.hardware_group_name
  , bs.oven_hardware_id
  , bs.session_id
  , bs.bootup_time
  , bs.local_bootup_time
  , bs.serial_number 
  , bs.customer_id
  , bs.oven_registration_id
  , bs.oven_generation
  , bs.key
  , bs.batch_number
  , bs.current_hardware_group
  , bs.is_internal_account
  , bs.is_test_oven
  , bc.firmwareverion AS firmware_version
  , LAG(bc.firmwareverion) OVER (PARTITION BY bc.deviceid
                                 ORDER BY bc.time_stamp) AS previous_firmware_version
  , previous_firmware_version IS NULL AS is_first_bootup
  , LEAD(bc.firmwareverion) OVER (PARTITION BY bc.deviceid
                                  ORDER BY bc.time_stamp) IS NULL AS is_most_recent_bootup 
  , NOT is_first_bootup AND (firmware_version <> previous_firmware_version) AS is_firmware_update
  , bc.freememory AS free_memory
  , COALESCE(reason, wakereason) AS raw_boot_reason
  -- FROM: https://developer.electricimp.com/api/hardware/wakereason
  , CASE WHEN raw_boot_reason = 'WAKEREASON_POWER_ON' OR raw_boot_reason = 'ESP_RST_POWERON' -- Powered on (cold boot)   
         THEN 'power_on'
         WHEN raw_boot_reason = 'WAKEREASON_SW_RESET' OR raw_boot_reason = 'ESP_RST_SW' -- Restarted due to a software reset (eg. with imp.reset()) or an out-of-memory error occurred
         THEN 'software_reset'  
         WHEN raw_boot_reason = 'WAKEREASON_NEW_SQUIRREL' -- Restarted due to new Squirrel code being loaded
         THEN 'code_update'
         WHEN raw_boot_reason = 'WAKEREASON_SQUIRREL_ERROR' -- Restarted due to a Squirrel run-time error  
         THEN 'code_error'
         WHEN raw_boot_reason = 'WAKEREASON_NEW_FIRMWARE' -- Restarted due to a firmware upgrade
         THEN 'new_firmware'
         WHEN raw_boot_reason = 'WAKEREASON_SNOOZE' -- Woken from a snooze-and-retry event
         THEN 'snooze_and_retry'
         WHEN raw_boot_reason = 'WAKEREASON_HW_RESET' -- Restarted by RESET_L pin (imp003 and above only)
         THEN 'hardware_reset' 
         WHEN raw_boot_reason = 'WAKEREASON_BLINKUP' -- Restarted following a reconfiguration by BlinkUp
         THEN 'blinkup_reconfiguration'
         WHEN raw_boot_reason = 'WAKEREASON_POWER_RESTORED' -- VBAT powered during a cold start  
         THEN 'power_restored'
         WHEN raw_boot_reason = 'WAKEREASON_SW_RESTART' -- Restarted by server.restart()
         THEN 'server_requested'
         --Match matching reasons with imp codes, new codes for new nexus reasons
         WHEN raw_boot_reason = 'ESP_RST_EXT'
         THEN 'ext'
         WHEN raw_boot_reason = 'ESP_RST_PANIC' 
         THEN 'panic'
         WHEN raw_boot_reason = 'ESP_RST_INT_WDT'
         THEN 'int_wdt'
         WHEN raw_boot_reason = 'ESP_RST_TASK_WDT' 
         THEN 'task_wdt'
         WHEN raw_boot_reason = 'ESP_RST_WDT' 
         THEN 'wdt'
         WHEN raw_boot_reason = 'ESP_RST_DEEPSLEEP' 
         THEN 'deepsleep'
         WHEN raw_boot_reason = 'ESP_RST_BROWNOUT' 
         THEN 'brownout'
         WHEN raw_boot_reason = 'ESP_RST_SDIO' 
         THEN 'sdio'
         WHEN raw_boot_reason = 'ESP_RST_USB' 
         THEN 'usb'
         WHEN raw_boot_reason = 'ESP_RST_JTAG'
         THEN 'jtag'
         WHEN raw_boot_reason = 'ESP_RST_EFUSE'
         THEN 'efuse'
         WHEN raw_boot_reason = 'ESP_RST_PWR_GLITCH'
         THEN 'pwr_glitch'
         WHEN raw_boot_reason = 'ESP_RST_CPU_LOCKUP' 
         THEN 'cpu_lockup'
         WHEN raw_boot_reason = 'ESP_RST_UNKNOWN' OR raw_boot_reason IS NULL
         THEN 'unknown_reason'
    END AS boot_reason
  , tco.oven_event_id IS NOT NULL AND COALESCE(boot_reason, '') IN ('power_on', 'power_restored') AS is_tco
  , bs.updated  
FROM base_source bs
LEFT JOIN {{ table_reference('oven_logs_bootup_complete') }} bc
  ON bs.oven_event_id = bc.oven_event_id
LEFT JOIN tco_cook_events tco
  ON bs.oven_event_id = tco.oven_event_id
