{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental", "retool"],
        incremental_strategy='merge'
    ) 
}}

-- Used by: Demand Planning/RTT - Hourly User State

SELECT 
  --Note: CHICAGO is a catch all due to an error being thrown when cycle 2 zips historically are not cycle 2 in upcoming_subterm_shipping table eg) cust 438082 for term 246
  {{ facility_network('uss.facility_network') }} AS facility_network
  , a.term_id
  , a.days_from_deadline
  , a.calendar_time_utc
  , a.calendar_time_chicago
  , SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_users
  , SUM(CASE WHEN is_paused THEN 1 ELSE 0 END) AS paused_users
  , SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) AS canceled_users
  , SUM(CASE WHEN is_skipped THEN 1 ELSE 0 END) AS skipped_users
  , SUM(CASE WHEN is_forced_skip THEN 1 ELSE 0 END) AS forced_skipped_users
  , SUM(CASE WHEN is_break_skip THEN 1 ELSE 0 END) AS break_skipped_users
  , skipped_users - forced_skipped_users AS net_skipped_users
  , AVG(order_size) AS avg_order_size
FROM {{ table_reference('hourly_user_state', 'season') }} a
LEFT JOIN {{ ref('upcoming_subterm_shipping') }} uss
  ON a.zip_cd = uss.zip_cd
  AND a.cycle = uss.cycle
  AND uss.term_id = {{ live_term(availability_check=True) }}
{% if is_incremental() %}
WHERE a.calendar_time_utc > (SELECT MAX(calendar_time_utc) FROM {{ this }})
{% else %}
WHERE a.calendar_time_utc >= (SELECT MIN(order_by_time) FROM {{ ref('terms') }} WHERE order_by_time >= DATEADD('month', -15, CURRENT_DATE()))
{% endif %}
GROUP BY 1,2,3,4,5
