
  SELECT
    s.customer_id
    , dtc.first_dtc_purchase_time
    , MIN(s.session_start_time) AS first_touch_time
    , MAX(s.session_start_time) AS last_touch_time
    , MIN_BY(lp.channel, s.session_start_time) AS first_touch_channel
    , MIN_BY(lp.channel_category, s.session_start_time) AS first_touch_channel_category
    , MAX_BY(lp.channel, s.session_start_time) AS last_touch_channel
    , MAX_BY(lp.channel_category, s.session_start_time) AS last_touch_channel_category
    , first_touch_time = last_touch_time AS has_same_first_last_channel
  FROM {{ ref('sessions') }} s
  INNER JOIN {{ ref('landing_pages') }} lp
    ON s.session_id = lp.session_id
  LEFT JOIN {{ ref('customer_dtc_ovens') }} dtc
    ON s.customer_id = dtc.customer_id
  --acquisition channel for both first/last touch is prior to the customer's first dtc oven purchase
  WHERE s.session_start_time <= dtc.first_dtc_purchase_time
  GROUP BY 1, 2
