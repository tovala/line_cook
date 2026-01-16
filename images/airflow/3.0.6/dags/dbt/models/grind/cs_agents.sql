
SELECT 
  zendesk_user_id
  , customer_id
  , name
  , role
  , email
  , phone_number
  , is_shared_phone_number
  , is_active 
  , is_suspended
  , is_moderator
  , is_verified
  , latest_login_time
  , notes
FROM {{ ref('cs_users') }}
WHERE is_employee 
