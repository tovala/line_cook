
SELECT 
  zsi.zipcode AS zip_cd
  , te.term_id
  , zsi.ship_period AS cycle
  , zsi.facility_network
  , zsi.shipping_company
  , zsi.shipping_service
  , zsi.shipping_origin
  , zsi.estimated_delivery_days
  , zsi.transit_days
  , zsi.ship_day_of_the_week
FROM {{ table_reference('zipcode_shipping_information') }} zsi
FULL OUTER JOIN {{ ref('terms') }} te
WHERE te.is_editable
  AND zsi.available