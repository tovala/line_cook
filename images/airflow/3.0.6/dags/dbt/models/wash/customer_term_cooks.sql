{{ 
    config(
        materialized='incremental',
        unique_key='customer_term_cook_id',
        tags=["thyme_incremental"]
    ) 
}}

SELECT 
  ce_sub.customer_id
  , {{ cook_event_type_cleaner() }} AS cook_event_type
  , te.term_id AS operational_term_id
  , {{ hash_natural_key('ce_sub.customer_id', 'cook_event_type', 'te.term_id') }} AS customer_term_cook_id
  , COALESCE(COUNT(DISTINCT ce_sub.cook_event_id), 0) AS total_cooks
  , COALESCE(COUNT(DISTINCT CASE WHEN ce_sub.cook_end_type = 'cook_completed' THEN ce_sub.cook_event_id END), 0) AS total_completed_cooks
  , MIN(ce_sub.cook_start_time)::DATE AS cook_type_cohort_date
  , MAX(ce_sub.cook_start_time)::DATE AS latest_cook_cycle_date
  , MAX(ce_sub.cook_start_time) AS latest_cook_cycle_timestamp
  , MIN(ce_sub.cook_start_time) AS earliest_cook_cycle_timestamp
FROM {{ ref('cook_events') }} ce_sub
INNER JOIN {{ ref('terms') }} te
  ON ce_sub.cook_start_time > te.start_date
  AND ce_sub.cook_start_time <= te.next_term_start_date
WHERE ce_sub.customer_id IS NOT NULL
  AND ce_sub.cook_event_subtype IS NOT NULL
{% if is_incremental() %}
  AND ce_sub.cook_start_time >= (SELECT start_date FROM {{ ref('terms') }} WHERE is_latest_completed)
{% endif %}
GROUP BY 1,2,3
