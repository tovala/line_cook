
WITH all_blessings AS (
-- 1. Get all blessed times, including historic
  (SELECT 
     device_id AS oven_hardware_id
     , blessed_at_time 
   FROM {{ source('brine', 'blessed_at') }})
     UNION 
  (SELECT 
     device_id AS oven_hardware_id
     , blessed_time AS blessed_at_time 
   FROM {{ table_reference('micro_logs_blessed') }})
), find_blessed_airvalas AS (
-- 2. Find all airvala ovens that have been blessed 
  (SELECT 
     device_id AS oven_hardware_id
   FROM {{ source('brine', 'blessed_airvalas') }})
     UNION 
  (SELECT DISTINCT
     device_id AS oven_hardware_id
   FROM {{ table_reference('micro_logs_blessed') }}
   WHERE model = 'airvala')
)
SELECT 
  ab.oven_hardware_id 
  , ab.blessed_at_time 
  , CASE WHEN blg.oven_hardware_id IS NOT NULL 
         THEN 'airvala'
         ELSE 'gen_2'
    END AS oven_generation
  , ROW_NUMBER() OVER (PARTITION BY ab.oven_hardware_id ORDER BY ab.blessed_at_time DESC) AS nth_blessing 
FROM all_blessings ab
LEFT JOIN find_blessed_airvalas blg 
ON ab.oven_hardware_id = blg.oven_hardware_id
