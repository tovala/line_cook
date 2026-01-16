
-- TO DO: implement unique address_id
-- TO DO: filter out addresses associated with Tovala employees and/or only include addresses that have been shipped to
SELECT 
  -- Creates a unique key when a user updates their address
  {{ hash_natural_key('l.userid', 'l.updated') }} AS address_id
   , l.userid AS customer_id
   , l.name AS user_name
   , l.phone AS phone_number
   , l.address_line1 AS line_1
   , l.address_line2 AS line_2
   , l.city AS city
   , l.state AS state
   , l.postal_code AS zip_cd
   , zsi.cycle as default_cycle
   , l.country AS country
   , COALESCE(l.address_type ILIKE 'Residential', FALSE) AS is_residential
   , l.ship_day_of_the_week AS ship_dow
   , zsi.delivery_day AS delivery_dow 
   , l.estimated_delivery_days
FROM {{ table_reference('location') }} l
LEFT JOIN {{ ref('zipcode_shipping_information') }} zsi 
   ON l.postal_code = zsi.zip_cd 
   AND zsi.is_available 
   AND zsi.is_default_cycle
