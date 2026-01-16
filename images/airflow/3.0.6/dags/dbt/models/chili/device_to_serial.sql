{{ 
    config(
        materialized='incremental',
        tags=["metadata"],
        unique_key='oven_hardware_id'
    ) 
}}
 
{% if is_incremental() %}
WITH incremental_data_source AS (
  SELECT 
    DISTINCT deviceid AS oven_hardware_id
  FROM {{ table_reference('oven_logs_combined', 'kinesis') }}
  WHERE time_stamp >= (SELECT MAX(latest_timestamp) FROM {{ this }}))
{% endif %}
SELECT
  deviceid AS oven_hardware_id
  -- Using LISTAGG means that if there are multiple valid serial numbers for a deviceid, the length of the final serialnumber will be > 13 - which we can test for
  , {{ clean_string("LISTAGG(DISTINCT CASE WHEN serialnumber LIKE 'TOVMN%' THEN SPLIT_PART(serialnumber, ';', -1) END, '|')") }} AS serial_number 
  , MAX(time_stamp) AS latest_timestamp
  , MAX_BY(source, time_stamp) AS latest_source
  , MAX_BY(devicegroupname, time_stamp) AS latest_hardware_group
FROM {{ table_reference('oven_logs_combined', 'kinesis') }}
-- This is the only device that has multiple serial numbers. Based on the ovens_history table, this is the correct serialnumber.
WHERE NOT (deviceid = '40000c2a691a0e3f' AND serialnumber <> 'TOVMN20113769')
{% if is_incremental() %}
  AND oven_hardware_id IN (SELECT oven_hardware_id FROM incremental_data_source)
{% endif %}
GROUP BY 1
