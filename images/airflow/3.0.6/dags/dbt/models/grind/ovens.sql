
WITH backend_serials AS (
  SELECT 
    device_id AS oven_hardware_id
    , {{ clean_string("LISTAGG(DISTINCT CASE WHEN serial_number LIKE 'TOVMN%' THEN SPLIT_PART(serial_number, ';', -1) END, '|')") }} AS serial_number
    , MIN(created) AS first_registration_time
  FROM {{ table_reference('ovens_history') }}  
  -- Special snowflake case
  WHERE NOT (serial_number = 'TOVMN200101' AND device_id = '40000c2a6917228a')
  GROUP BY 1
), all_devices AS (
  (SELECT oven_hardware_id FROM backend_serials)
   UNION 
  (SELECT oven_hardware_id FROM {{ source('chili', 'device_to_serial') }})
)
SELECT 
  ad.oven_hardware_id
  , COALESCE(dts.serial_number, bs.serial_number) AS serial_number
  , CASE WHEN ad.oven_hardware_id LIKE '30000%'
         THEN 'gen_1'
         WHEN ad.oven_hardware_id LIKE '40000%' OR ad.oven_hardware_id LIKE '5000%'
         THEN CASE WHEN COALESCE(dts.serial_number, bs.serial_number) LIKE 'TOVMN2%'
                   THEN 'gen_2'
                   WHEN COALESCE(dts.serial_number, bs.serial_number) LIKE 'TOVMN3%'
                   THEN 'airvala'
                   WHEN bt.oven_generation IS NOT NULL 
                   THEN bt.oven_generation
                   WHEN dts.latest_source = 'airvala'
                   THEN 'airvala'
                   ELSE 'gen_2'
              END 
    END AS oven_generation 
  , REGEXP_SUBSTR(COALESCE(dts.serial_number, bs.serial_number), 'TOVMN\\d{4}') AS build_id
  , TRY_TO_NUMERIC(REGEXP_SUBSTR(build_id, 'TOVMN[2-3](\\d{3})', 1, 1, 'e')) AS batch_number
  , COALESCE(batch_number BETWEEN 54 AND 65 AND build_id LIKE 'TOVMN2%', FALSE) AS is_basketvala
  , bt.blessed_at_time 
  , ros.retailer AS oven_retailer
  , ros.show_code AS costco_show_code
  , dts.latest_hardware_group AS current_hardware_group
  , tov.device_id IS NOT NULL AS is_oven_registered
  -- If we can't find a registration time, use the earliest event time
  , LEAST(bs.first_registration_time, COALESCE(tov.created, '9999-12-31')) AS first_oven_registration_time 
FROM all_devices ad 
LEFT JOIN {{ source('chili', 'device_to_serial') }} dts
  ON ad.oven_hardware_id = dts.oven_hardware_id
LEFT JOIN backend_serials bs 
  ON ad.oven_hardware_id = bs.oven_hardware_id
LEFT JOIN {{ ref('blessed_time') }} bt
  ON ad.oven_hardware_id = bt.oven_hardware_id
  AND bt.nth_blessing = 1
LEFT JOIN {{ ref('retail_oven_serial_numbers') }} ros 
  ON COALESCE(dts.serial_number, bs.serial_number) = ros.serial_number 
LEFT JOIN {{ ref('tovala_devices') }} tov 
  ON ad.oven_hardware_id = tov.device_id
-- This excludes any of the '006' testing devices which are not actual ovens 
WHERE RLIKE(ad.oven_hardware_id, '(30000|40000|5000).*')
