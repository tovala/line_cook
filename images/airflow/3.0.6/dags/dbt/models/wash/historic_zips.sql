
WITH all_possible_zips AS (
(SELECT 
   userid AS customer_id
   , shipping_zip AS zip_code
   , created AS zip_time
 FROM {{ table_reference('third_party_purchases') }} 
 WHERE shipping_zip IS NOT NULL)
UNION ALL 
(SELECT 
   userid AS customer_id
   , shipping_zip AS zip_code
   , created AS zip_time
 FROM {{ table_reference('products_shipment') }}
 WHERE shipping_zip IS NOT NULL)
UNION ALL 
(SELECT 
   userid AS customer_id
   , shipping_zip AS zip_code
   , created AS zip_time
 FROM {{ table_reference('orderfulfillment') }}
 WHERE shipping_zip IS NOT NULL
 -- Edge cases. These zip codes do not exist.
 )
UNION ALL 
(SELECT 
   userid AS customer_id
   , postal_code AS zip_code
   , updated AS zip_time
 FROM {{ table_reference('location') }}
 WHERE postal_code IS NOT NULL
   -- Test users (aka Testy mctesticles)
   AND NOT userid IN (16266, 13237, 10964, 6986))
UNION ALL 
(SELECT DISTINCT
   userid AS customer_id
   , COALESCE(REGEXP_SUBSTR(shippingaddress, '\\d{5}$')
              , SPLIT_PART(REGEXP_SUBSTR(shippingaddress, 'PostalCode:\\d{5}'), ':', 2)) AS zip_code
   , updated AS zip_time
 FROM {{ table_reference('ovenorderfulfillment') }}
 WHERE shippingaddress IS NOT NULL)
), lagged_zips AS (
 SELECT 
   customer_id 
   -- Seems like for a time we didn't validate zips so there are some odd artifacts in the DB
   , CASE WHEN zip_code = '60524' AND customer_id = 635
          THEN '60527'
          WHEN zip_code = '62929' AND customer_id = 657
          THEN '62959'
          WHEN zip_code = '9410' AND customer_id = 992
          THEN '94105'
          WHEN zip_code = '94113' AND customer_id = 1678
          THEN '94133'
          WHEN zip_code = '44780' AND customer_id = 6221
          THEN '44708'
          WHEN zip_code = '702' AND customer_id = 671
          THEN '89135'
          WHEN zip_code = '2459.0' AND customer_id = 1012
          THEN '02459'
          WHEN zip_code = '22390' AND customer_id = 287372
          THEN '22309'
          WHEN zip_code = '60898' AND customer_id = 15717
          THEN '60098'
          WHEN zip_code IN ('05005', '5005') AND customer_id = 15656
          THEN '08005'
          WHEN zip_code = '27266' AND customer_id = 14202
          THEN '27263'
          WHEN zip_code = '33718' AND customer_id = 13974
          THEN '33710'
          WHEN zip_code = '32019' AND customer_id = 13286
          THEN '32819'
          WHEN zip_code = '74040' AND customer_id = 11601
          THEN '74070'
          WHEN zip_code = '37124' AND customer_id = 10225
          THEN '37214'
          WHEN zip_code = '99230' AND customer_id = 40945
          THEN '99203'
          WHEN zip_code = '34707' AND customer_id = 6228
          THEN '33707'
          WHEN zip_code LIKE '%.%'
          THEN SPLIT_PART(zip_code, '.', 0)
          WHEN zip_code LIKE '%-%'
          THEN SPLIT_PART(zip_code, '-', 0)
          WHEN LEN(zip_code) = 4 
          THEN '0' || zip_code 
          ELSE zip_code 
     END AS zip_cd 
   , LAG(zip_cd) OVER (PARTITION BY customer_id ORDER BY zip_time) AS previous_zip
   , zip_time
 FROM all_possible_zips
)
SELECT 
  lz.customer_id 
  , lz.zip_cd
  , tz.timezone
  , tz.state
  -- For the first row, set the start time to be the users first active status time IF there is no earlier zip available
  , CASE WHEN lz.previous_zip IS NULL AND fus.status_start_time < lz.zip_time  
         THEN fus.status_start_time 
         ELSE lz.zip_time  
    END AS zip_start_time
  , COALESCE(LEAD(lz.zip_time) OVER (PARTITION BY lz.customer_id 
                                     ORDER BY lz.zip_time), '9999-12-31') AS zip_end_time
  , lz.previous_zip IS NULL AS is_first_row
FROM lagged_zips lz
LEFT JOIN {{ ref('zips_to_timezone') }} tz
  ON lz.zip_cd = tz.zip_cd
LEFT JOIN {{ ref('first_user_statuses') }} fus 
  ON lz.customer_id = fus.customer_id 
WHERE lz.zip_cd <> COALESCE(lz.previous_zip, '')
