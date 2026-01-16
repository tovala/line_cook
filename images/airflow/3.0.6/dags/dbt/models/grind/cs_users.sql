
WITH 
customer_emails AS ( -- Get all customer ID's and emails from customers table 
  SELECT DISTINCT
    customer_id
    , email 
    , registration_time 
    , is_employee
  FROM {{ ref('customers') }}
  WHERE email IS NOT NULL
), 
customer_phones AS ( -- Get all customer ID's and phone numbers from addresses table 
  SELECT DISTINCT
    a9.customer_id 
    , a9.phone_number
    , c9.registration_time
  FROM {{ ref('addresses') }} a9 
  INNER JOIN {{ ref('customers') }} c9 
    ON a9.customer_id = c9.customer_id 
  WHERE phone_number IS NOT NULL
),
zendesk_users AS ( -- Get data for all Zendesk users 
  SELECT 
    id AS zendesk_user_id 
    , name 
    , role 
    , email
    , phone AS phone_number 
    , COALESCE(shared_phone_number, FALSE) AS is_shared_phone_number
    , active AS is_active 
    , suspended AS is_suspended
    , moderator AS is_moderator 
    , verified AS is_verified
    , last_login_at AS latest_login_time 
    , notes 
  FROM {{ source('zendesk_support_v3', 'users') }}
), 
zendesk_email_users AS ( -- For Zendesk users that have an email address, associate them with their Tovala Customer ID
  SELECT DISTINCT 
    e.customer_id 
    , e.is_employee 
    , u.* 
  FROM ( -- must have email address
    SELECT * 
    FROM zendesk_users 
    WHERE email IS NOT NULL 
  ) u 
  INNER JOIN customer_emails e 
    ON u.email = e.email 
  WHERE NOT EXISTS ( -- if multiple customer ID's exist with the same email address, use the most recently registered Customer ID
    SELECT 1 
    FROM customer_emails e9
    WHERE e9.email = e.email 
    AND e9.registration_time > e.registration_time
  )
), 
zendesk_phone_users AS ( -- For Zendesk users that don't have email address, but have a phone number, associate them with their Tovala Customer ID 
  SELECT DISTINCT 
    e.customer_id
    , e.is_employee 
    , u.* 
  FROM ( -- no email address, but has phone number 
    SELECT * 
    FROM zendesk_users 
    WHERE email IS NULL 
    AND phone_number IS NOT NULL 
  ) u 
  INNER JOIN customer_phones p 
    ON u.phone_number = p.phone_number 
  INNER JOIN customer_emails e 
    ON p.customer_id = e.customer_id 
  WHERE NOT EXISTS ( -- if multiple customers exist for the same phone number, use the most recently registered Customer ID
    SELECT 1 
    FROM customer_phones cp 
    WHERE cp.phone_number = p.phone_number 
    AND cp.registration_time > p.registration_time
  )
), 
merged_zendesk_user_data AS ( -- UNION together data for Zendesk users who had email with those who only had phone number
  SELECT * 
  FROM zendesk_email_users zeu 
  UNION 
  SELECT * 
  FROM zendesk_phone_users zpu 
  -- Only use phone number data to associate Zendesk users to Customer IDs if they are not already associated using email address
  WHERE zpu.customer_id NOT IN (SELECT customer_id FROM zendesk_email_users)
)
SELECT 
  zendesk_user_id 
  , customer_id 
  , is_employee
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
FROM merged_zendesk_user_data
