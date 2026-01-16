SELECT 
  m.userid AS customer_id 
, m.membership_type_id AS membership_group
, m.status = 'active' AS is_active
, CASE WHEN is_active THEN NULL ELSE m.updated END AS cancellation_time
FROM {{ table_reference('memberships') }} m 
INNER JOIN {{ table_reference('membership_types') }} mt 
  ON mt.id = m.membership_type_id
