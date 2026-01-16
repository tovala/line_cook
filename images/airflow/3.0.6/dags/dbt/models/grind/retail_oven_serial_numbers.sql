
WITH serial_roadshow_map AS (
  SELECT 
    serial_number
    , roadshow_location 
  FROM {{ table_reference('oven_roadshows') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY serial_number ORDER BY position_index DESC) = 1
)
SELECT 
  ohi.serial_number 
  , ohi.model
  , ohi.date_shipped::DATE AS ship_date
  , LOWER(ohi.channel) AS retailer
  , srm.roadshow_location AS internal_roadshow_id
  , cr.show_code
FROM {{ table_reference('oven_hardware_information') }} ohi
LEFT JOIN serial_roadshow_map srm 
  ON ohi.serial_number = srm.serial_number
LEFT JOIN {{ source('mold', 'costco_roadshows') }} cr 
  ON srm.roadshow_location = cr.internal_roadshow_id
