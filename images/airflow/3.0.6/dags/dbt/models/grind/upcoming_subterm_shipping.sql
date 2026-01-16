
WITH subterm_shipping AS (
  SELECT 
    COALESCE(zo.zip_cd, zd.zip_cd) AS zip_cd
    , COALESCE(zo.term_id, zd.term_id) AS term_id
    , COALESCE(zo.cycle, zd.cycle) AS cycle
    , COALESCE(zo.facility_network, zd.facility_network) AS facility_network
    , COALESCE(zo.shipping_company, zd.shipping_company) AS shipping_company
    , COALESCE(zo.shipping_service, zd.shipping_service) AS shipping_service
    , COALESCE(zo.shipping_origin, zd.shipping_origin) AS shipping_origin
    , COALESCE(zo.estimated_delivery_days, zd.estimated_delivery_days) AS estimated_delivery_days
    , COALESCE(zo.transit_days, zd.transit_days) AS transit_days
    , COALESCE(zo.ship_day_of_the_week, zd.ship_day_of_the_week) AS ship_day_of_the_week
  FROM {{ ref('upcoming_subterm_default_shipping') }} zd 
  LEFT JOIN {{ ref('upcoming_subterm_override_shipping') }} zo 
    ON zo.term_id =  zd.term_id
    AND zo.cycle = zd.cycle 
    AND zo.zip_cd = zd.zip_cd)
SELECT 
    {{ hash_natural_key('ss.zip_cd', 'st.subterm_id') }} AS upcoming_subterm_shipping_id
    , ss.zip_cd
    , ss.term_id
    , st.subterm_id
    , ss.cycle
    , ss.facility_network
    , ss.shipping_company
    , ss.shipping_service
    , ss.shipping_origin
    , ss.estimated_delivery_days
    , ss.transit_days
    , ss.ship_day_of_the_week
FROM subterm_shipping ss
INNER JOIN {{ ref('subterms') }} st 
  ON st.cycle = ss.cycle
  AND st.facility_network = ss.facility_network
  AND st.term_id = ss.term_id
WHERE st.is_available
