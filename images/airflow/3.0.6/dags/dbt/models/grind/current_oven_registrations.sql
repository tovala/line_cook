
SELECT
    o.id AS oven_registration_id
    , o.userid AS customer_id 
    , roh.first_customer_registration_time AS registration_time 
    , roh.oven_generation
    , o.name AS oven_name
    , roh.oven_retailer
    , roh.costco_show_code
    , roh.oven_hardware_id
    , roh.serial_number 
    , roh.build_id
    , ip.current_ip_address
FROM {{ table_reference('ovens') }} o 
INNER JOIN {{ ref('customers') }} c -- Exclude test customers (and their ovens)
  ON o.userid = c.customer_id 
LEFT JOIN {{ ref('historic_oven_registrations') }} roh
  ON o.id = roh.oven_registration_id 
  AND roh.is_most_recent_record
LEFT JOIN {{ ref('ip_addresses') }} ip
  ON roh.oven_hardware_id = ip.oven_hardware_id