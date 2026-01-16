{{
  config(
    pre_hook='SHOW USERS'
  )
}}
SELECT
  "name" AS name
  , "created_on" AS created_on
  , "login_name" AS login_name
  , "display_name" AS display_name
  , "first_name" AS first_name
  , "last_name" AS last_name
  , "email" AS email
  , "mins_to_unlock" AS mins_to_unlock
  , "days_to_expiry" AS days_to_expiry
  , "comment" AS comment
  , "disabled" AS disabled
  , "must_change_password" AS must_change_password
  , "snowflake_lock" AS snowflake_lock
  , "default_warehouse" AS default_warehouse
  , "default_namespace" AS default_namespace
  , "default_role" AS default_role
  , "default_secondary_roles" AS default_secondary_roles
  , "ext_authn_duo" AS ext_authn_duo
  , "ext_authn_uid" AS ext_authn_uid
  , "mins_to_bypass_mfa" AS mins_to_bypass_mfa
  , "owner" AS owner
  , "last_success_login" AS last_success_login
  , "expires_at_time" AS expires_at_time
  , "locked_until_time" AS locked_until_time
  , "has_password" AS has_password
  , "has_rsa_public_key" AS has_rsa_public_key
  , "type" AS type
  , "has_mfa" AS has_mfa
FROM TABLE(RESULT_SCAN())
