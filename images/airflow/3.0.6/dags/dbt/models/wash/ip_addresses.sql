
SELECT 
  deviceid AS oven_hardware_id 
  , MAX_BY(ipaddress, time_stamp) AS current_ip_address
FROM {{ table_reference('oven_logs_ovenconnected') }}
WHERE ipaddress IS NOT NULL 
  -- ipaddress was not available prior to this, for efficiency limiting the query
  AND time_stamp > '2024-05-01'
GROUP BY ALL
