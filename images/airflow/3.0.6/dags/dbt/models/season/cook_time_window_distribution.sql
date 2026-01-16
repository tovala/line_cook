
SELECT
  COALESCE(tmce.meal_sku_id::STRING, tmce.add_on_id) AS meal_or_addon_id
  , tmce.add_on_id
  , tmce.meal_sku_id
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'early_breakfast' THEN ces.cook_event_id END)) AS early_breakfast_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'avg_breakfast' THEN ces.cook_event_id END)) AS avg_breakfast_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'late_breakfast' THEN ces.cook_event_id END)) AS late_breakfast_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'early_lunch' THEN ces.cook_event_id END)) AS early_lunch_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'avg_lunch' THEN ces.cook_event_id END)) AS avg_lunch_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'late_lunch' THEN ces.cook_event_id END)) AS late_lunch_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'early_dinner' THEN ces.cook_event_id END)) AS early_dinner_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'avg_dinner' THEN ces.cook_event_id END)) AS avg_dinner_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category = 'late_dinner' THEN ces.cook_event_id END)) AS late_dinner_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category ilike '%breakfast%' THEN ces.cook_event_id END)) AS breakfast_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category ilike '%lunch%' THEN ces.cook_event_id END)) AS lunch_cook_count
  , COUNT( DISTINCT (CASE WHEN ces.time_of_meal_category ilike '%dinner%' THEN ces.cook_event_id END)) AS dinner_cook_count
FROM {{ ref('tovala_meal_cook_events') }} tmce
LEFT JOIN {{ table_reference('cook_event_starts', 'wash') }} ces
  ON tmce.cook_event_id = ces.cook_event_id
WHERE tmce.is_valid_scan
AND NOT ces.is_internal_account
AND NOT ces.is_test_oven
AND NOT ces.is_test_event
GROUP BY 1, 2, 3
