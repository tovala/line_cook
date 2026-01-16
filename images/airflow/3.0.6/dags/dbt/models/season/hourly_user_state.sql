{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='hourly_user_state_id'
    ) 
}}

WITH term_points AS (
  SELECT 
    t.term_id
    , TIMESTAMPDIFF('day', tp.calendar_time_chicago, CONVERT_TIMEZONE('America/Chicago', t.order_by_time)) AS days_from_deadline
    , tp.calendar_time_utc
    , tp.calendar_time_chicago
  FROM {{ source('brine', 'dim_calendar_timestamps') }} tp
  INNER JOIN {{ ref('terms') }} t
    ON tp.calendar_time_utc <= CONVERT_TIMEZONE('UTC',t.order_by_time)
    AND tp.calendar_time_utc >= DATEADD('day', -21, CONVERT_TIMEZONE('UTC',t.order_by_time))
  {% if is_incremental() %}
  WHERE tp.calendar_time_utc >= (SELECT MAX(calendar_time_utc) FROM {{ this }})
  {% else %}
  WHERE tp.calendar_time_utc >= (SELECT order_by_time FROM {{ ref('terms') }} WHERE term_id = 167) --using Term 167 since that is when we started having accurate skip records
  {% endif %}
  AND tp.calendar_time_utc <= DATEADD('hour', -2, {{ current_timestamp_utc() }})
)
SELECT 
  {{ hash_natural_key('uas.customer_id', 'tp.calendar_time_utc', 'tp.term_id') }} AS hourly_user_state_id
  , uas.customer_id
  , tp.term_id
  , tp.days_from_deadline
  , tp.calendar_time_utc
  , tp.calendar_time_chicago
  , hz.zip_cd
  , uas.user_status = 'active' AS is_active
  , uas.user_status = 'paused' AS is_paused
  , uas.user_status = 'canceled' AS is_canceled
  , se.skip_id IS NOT NULL AS is_skipped
  , fms.first_meal_selection_time IS NOT NULL AS has_selected_meal
  , se.is_forced_skip
  , se.is_break_skip
  , CASE WHEN uas.user_status = 'active' AND se.skip_id IS NULL 
         THEN COALESCE(tov.override_order_size, uas.default_order_size)
    END AS order_size
  , CASE WHEN uas.user_status = 'active' AND se.skip_id IS NULL 
         THEN COALESCE(tov.override_cycle, uas.cycle, 1)
         ELSE COALESCE(uas.cycle
                       , LAG(uas.cycle) IGNORE NULLS OVER (PARTITION BY uas.customer_id ORDER BY uas.status_start_time)
                       , c.default_cycle)
    END AS cycle
  , COALESCE(uas.wants_double_autofill, FALSE) AS wants_double_autofill
  , COALESCE(uas.wants_breakfast_autofill, FALSE) AS has_breakfast_autofill
  , COALESCE(uas.wants_surcharged_autofill, FALSE) AS has_surcharged_autofill
FROM term_points tp
LEFT JOIN {{ ref('full_user_statuses') }} uas
  ON uas.status_start_time <= tp.calendar_time_utc AND tp.calendar_time_utc < COALESCE(uas.status_end_time, '9999-12-31')
LEFT JOIN {{ ref('combined_skip_events') }} se
  ON tp.term_id = se.term_id    
  AND uas.customer_id = se.customer_id
  AND se.skip_time <= tp.calendar_time_utc AND tp.calendar_time_utc < COALESCE(se.unskip_time, '9999-12-31')
LEFT JOIN {{ ref('term_overrides_historic') }} tov
  ON tp.term_id = tov.term_id
  AND uas.customer_id = tov.customer_id
  AND tov.override_start_time <= tp.calendar_time_utc AND tp.calendar_time_utc < COALESCE(tov.override_end_time, '9999-12-31')
LEFT JOIN {{ ref('first_meal_orders') }} fmo
  ON uas.customer_id = fmo.customer_id
  AND fmo.order_time < tp.calendar_time_utc
LEFT JOIN {{ ref('historic_zips') }} hz 
  ON uas.customer_id = hz.customer_id 
  AND tp.calendar_time_utc  >= hz.zip_start_time 
  AND tp.calendar_time_utc < COALESCE(hz.zip_end_time, '9999-12-31')
LEFT JOIN {{ ref('first_meal_selections') }} fms
  ON uas.customer_id = fms.customer_id
  AND tp.term_id = fms.term_id
  AND fms.first_meal_selection_time <= tp.calendar_time_utc
  AND NOT fms.is_autoselection
INNER JOIN {{ ref('customers') }} c --To filter out leads and internal accounts
  ON uas.customer_id = c.customer_id 
WHERE NOT c.is_internal_account 
  AND (fmo.meal_order_id IS NOT NULL OR uas.user_status = 'active') --To make sure we only look at customers who have had a meal order or are active for that term
