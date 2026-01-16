
SELECT 
    u.id AS user_id
    , u.email 
FROM {{ table_reference('users') }} u 
WHERE u.email ILIKE '%testvala%'
  OR u.email ILIKE '%test_vala%'
