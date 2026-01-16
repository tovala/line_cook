
SELECT 
  m.userid AS customer_id 
  , BOOLOR_AGG(mt.free_delivery) AS gets_free_delivery
FROM {{ table_reference('memberships') }} m 
INNER JOIN {{ table_reference('membership_types') }} mt 
  ON mt.id = m.membership_type_id
WHERE m.membership_type_id ILIKE '%grandfathered%'
  AND mt.active
GROUP BY 1
