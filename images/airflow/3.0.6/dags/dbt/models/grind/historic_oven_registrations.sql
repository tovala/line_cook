
SELECT 
  -- oven_history_id is a hash of the ovenid and created timestamp in the ovens_history table
  oh.ovens_history_id
  , oh.device_id AS oven_hardware_id
  , di.serial_number
  , di.oven_generation
  , di.build_id
  , di.batch_number
  , di.is_basketvala
  , di.blessed_at_time
  , di.oven_retailer
  , di.costco_show_code
  , di.current_hardware_group
  , di.is_oven_registered
  , di.first_oven_registration_time
  -- oven_registration_id is a foreign key on ovens and ovens_tovala iff the oven is currently registered and all the information about the oven is unchanged
  , COALESCE(td.id, oh.ovenid) AS oven_registration_id  
  , oh.userid AS customer_id
  , oh.name AS oven_name
  , ROW_NUMBER() OVER (PARTITION BY oh.device_id ORDER BY oh.created) AS row_num
  , oh.created AS start_time
  , LEAD(oh.created) OVER (PARTITION BY oh.device_id ORDER BY oh.created) AS end_time
  , LEAD(oh.created) OVER (PARTITION BY oh.userid ORDER BY oh.created) AS customer_end_time
  , l.lead_id IS NULL OR ut.userid IS NOT NULL AS is_internal_account
  , end_time IS NULL AS is_most_recent_record
  , customer_end_time IS NULL AS is_most_recent_record_for_customer
  -- INCLUDED FOR TESTING
  , ROW_NUMBER() OVER (PARTITION BY oh.userid ORDER BY oh.created) = 1 AS is_first_record_for_customer
  , FIRST_VALUE(oh.created) OVER (PARTITION BY oh.userid, oh.device_id ORDER BY oh.created ASC) AS first_customer_registration_time
FROM {{ table_reference('ovens_history') }} oh
LEFT JOIN {{ ref('ovens') }} di 
  ON oh.device_id = di.oven_hardware_id
-- There are early records where the initial oven_registration_id did not match ovens_history - this fixes that
LEFT JOIN {{ ref('tovala_devices') }} td 
  ON oh.ovenid <> td.id 
  AND oh.device_id = td.device_id 
  AND oh.plan_id = td.plan_id
  AND td.id NOT IN (SELECT DISTINCT ovenid FROM {{ table_reference('ovens_history') }})
LEFT JOIN {{ ref('leads') }}  l
  ON oh.userid  = l.lead_id
LEFT JOIN {{ table_reference('usertags') }} ut 
  ON ut.userid = oh.userid 
  AND ut."TAG" = 'internal_account' 
WHERE oh.action <> 'DELETE'
  -- This explicitly excludes any test devices that do not follow the correct format for device_id
  AND RLIKE(oh.device_id, '(30000|40000|5000).*')
