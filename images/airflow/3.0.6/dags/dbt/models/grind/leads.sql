
SELECT 
    u.id AS lead_id
    , u.registered AS registration_time
    , u.name
    , u.email AS email 
    , UPPER(u.referral_code) AS referral_cd
    , u.is_employee
FROM {{ table_reference('users') }} u 
LEFT JOIN {{ ref('internal_accounts') }} ia 
    ON u.id = ia.user_id 
WHERE ia.user_id IS NULL 
