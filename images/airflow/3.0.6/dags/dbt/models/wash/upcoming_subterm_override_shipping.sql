
SELECT 
  stz.zipcode AS zip_cd
  , st.term_id
  , st.cycle
  , st.facility_network
  , szsi.shipping_company
  , szsi.shipping_service
  , szsi.shipping_origin
  , szsi.estimated_delivery_days
  , szsi.transit_days
  , szsi.ship_day_of_the_week
FROM {{ table_reference('subterm_zipcodes') }} stz 
INNER JOIN {{ ref('subterms') }} st 
  ON st.subterm_id = stz.subterm_id
INNER JOIN {{ ref('terms') }} te 
  ON st.term_id = te.term_id
LEFT JOIN {{ table_reference('subterm_zipcodes_shipping_information') }} szsi 
  ON szsi.subterm_zipcode_id = stz.id
WHERE te.is_editable
  AND stz.available
