
SELECT
  hus.customer_id
  , {{ facility_network('uss.facility_network') }} AS current_facility_network
  , hus.zip_cd
  , hus.cycle
  , CASE WHEN hus.is_active = TRUE THEN 'active'
         WHEN hus.is_paused = TRUE THEN 'paused'
         WHEN hus.is_canceled = TRUE THEN 'canceled' 
    END AS current_status
  , hus.is_skipped
FROM {{ table_reference('hourly_user_state', 'season') }} hus
LEFT JOIN {{ref( 'upcoming_subterm_shipping' )}} uss
  ON hus.cycle = uss.cycle
  AND hus.zip_cd = uss.zip_cd
  AND hus.term_id = uss.term_id
WHERE hus.term_id = (SELECT MIN(t.term_id) FROM {{ ref('terms') }} t WHERE NOT is_past_order_by AND NOT is_partially_unavailable)
  AND hus.calendar_time_utc = (SELECT MAX(calendar_time_utc) FROM {{ table_reference('hourly_user_state', 'season') }})
