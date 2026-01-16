
WITH meal_counts AS (
  SELECT 
    ms.customer_id
    , ms.meal_sku_id 
    , ms.term_id 
    , ms.meal_order_id
    , SUM(msk.serving_count) as count_servings
  FROM {{ ref('meal_selections') }} ms 
  INNER JOIN {{ ref('meal_skus') }} msk 
    ON ms.meal_sku_id = msk.meal_sku_id
  WHERE ms.is_fulfilled
  GROUP BY 1,2,3,4
), ranked_cook_events AS (
  SELECT 
    cec.cook_event_id
    , cec.cook_cycle_id
    , cec.unique_cook_cycle_id
    , cec.oven_hardware_id
    , cec.oven_generation
    , cec.hardware_group_name
    , cec.serial_number
    , cec.customer_id 
    , cec.session_id
    , cec.cook_start_time
    , cec.cook_end_time
    , cec.cook_end_type
    , cec.cook_duration_seconds
    , cec.barcode 
    , cec.cook_routine
    , cec.cook_routine_duration
    , cec.uses_broil 
    , cec.uses_steam
    , cec.normalized_time_remaining_seconds
    , {{ meal_sku_id('cec.barcode') }} AS meal_sku_id
    , ao.add_on_id
    -- There are a handful of malformed barcodes, this eliminates them
    , CASE WHEN ARRAY_SIZE(split(barcode, '|')) <= 4
           THEN NULLIF(TRIM(SPLIT_PART(cec.barcode, '|', 4)), '') 
      END AS subroutine
    , ms.term_id 
    , (nt.meal_sku_id IS NOT NULL) AS is_qvc_meal
    -- NOTE: these fields are used to determine if a meal scan event is likely to be a tovala meal being cooked and shouldn't be used in analyses
    , COALESCE(DATEDIFF('days', te.start_date, cec.cook_start_time) BETWEEN 0 AND 31, FALSE) AS is_within_month
    , ROW_NUMBER() OVER (
        PARTITION BY 
          cec.customer_id, 
          {{ meal_sku_id('cec.barcode') }}, 
          ms.term_id, 
          cec.cook_end_type, 
          is_within_month
        ORDER BY cec.cook_start_time
      ) AS cook_number
  FROM {{ ref('cook_events') }} cec
  LEFT JOIN {{ ref('meal_skus') }} ms
    ON {{ meal_sku_id('cec.barcode') }} = ms.meal_sku_id
  LEFT JOIN {{ source('brine', 'non_tovala_meal_ids') }} nt
    ON {{ meal_sku_id('cec.barcode') }} = nt.meal_sku_id 
    AND nt.category = 'qvc'
  LEFT JOIN {{ ref('terms') }} te 
    ON te.term_id = ms.term_id 
  LEFT JOIN {{ ref('add_ons') }} ao
    ON NULLIF(TRIM(SPLIT_PART(cec.barcode, '|', 2)), '') = ao.short_tag
  WHERE cec.cook_event_subtype = 'tovala_meal'
)
SELECT
  rce.cook_event_id  
  , rce.cook_cycle_id
  , rce.unique_cook_cycle_id
  , rce.oven_hardware_id
  , rce.oven_generation
  , rce.hardware_group_name
  , rce.serial_number
  , rce.customer_id 
  , rce.session_id
  , rce.cook_start_time
  , rce.cook_end_time
  , rce.cook_end_type
  , rce.cook_duration_seconds
  , rce.barcode 
  , rce.cook_routine
  , rce.cook_routine_duration
  , rce.uses_broil 
  , rce.uses_steam
  -- NOTE: sometimes this value is off, usually by about a second. For consistency's sake, we are setting these numbers to sum to 0
  , rce.normalized_time_remaining_seconds
  , rce.meal_sku_id
  , rce.add_on_id
  , CASE WHEN rce.subroutine = 'UNDEFINED' AND rce.meal_sku_id IN (2147, 2122) 
         THEN 'A' -- Have verified that in these edge cases, the 'UNDEFINED' cook_routine corresponds to the 'A' cook_routine
         ELSE rce.subroutine
    END AS subroutine
  , rce.term_id 
  , rce.is_qvc_meal
  -- Meal order id is not null for customers who ordered that meal at all, including if they had cooked the meals they had ordered
  , mc.meal_order_id
  -- Logic: (1) customer has not completed a cook event for more meals than they have ordered AND have actually ordered the meal in question
  --        (2) Cook event was completed 
  --        (3) Cook event is within 31 days of the cycle 1 ship date for that term 
  --        (4) All add-on meals
  , (rce.cook_number <= COALESCE(mc.count_servings, -1) AND rce.is_within_month AND rce.cook_end_type = 'cook_completed') 
    OR (rce.meal_sku_id IN (SELECT add_on_meal_id FROM {{ ref('coupled_add_ons') }}))
    OR rce.add_on_id IS NOT NULL AS is_valid_scan
  -- NOTE: these are included for debugging/testing purposes. 
  , rce.is_within_month
  , rce.cook_number
FROM ranked_cook_events rce
LEFT JOIN meal_counts mc 
  ON rce.customer_id = mc.customer_id 
  AND rce.meal_sku_id = mc.meal_sku_id
  AND rce.term_id = mc.term_id
