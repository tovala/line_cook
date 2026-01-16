SELECT
  id AS action_id
  , actiondatetime AS action_time
  , action
  , TRY_TO_NUMERIC(adspotid)::INTEGER AS spot_id
  , creative
  , network AS tv_network
  , actionprobability AS action_probability
  , actionsessionprobability AS action_session_probability
  , attributedrevenue AS attributed_revenue
  , TRY_TO_NUMERIC(customeruserref)::INTEGER AS customer_id
  , referrer AS site_visit_referrer
  , TRY_TO_NUMERIC(usersessionid)::INTEGER AS user_session_id
  , spottime AS spot_time
  , region
  , CASE 
      WHEN region LIKE 'US/east-coast/%' THEN 'America/New_York'
      WHEN region LIKE 'US/central/%' THEN 'America/Chicago'
      WHEN region LIKE 'US/mountain/%' THEN 'America/Denver'
      WHEN region LIKE 'US/west-coast/%' THEN 'America/Los_Angeles'
      ELSE 'America/Chicago' -- default to Central if region is unknown
    END AS source_timezone
  --Dates are set to the timezone of the portal, which is NYC in our case
  , CONVERT_TIMEZONE('America/New_York', 'UTC', action_time) AS action_time_utc
  , CONVERT_TIMEZONE('America/New_York', 'UTC', spot_time) AS spot_time_utc
  , t_adspots_datagroup AS data_group
FROM {{ table_reference('tvsquared_actions') }}
WHERE action = 'ordercompleted'

