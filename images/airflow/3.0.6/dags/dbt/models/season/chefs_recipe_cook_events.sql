
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
  , cec.cook_duration_seconds
  , cec.cook_end_type
  -- Normalizing the barcode so this can be joined to the tovala_assist_recipe table in Looker
  , CASE WHEN tab.barcode IS NOT NULL 
         THEN tab.barcode 
         ELSE cec.barcode
    END AS barcode
  , cec.normalized_time_remaining_seconds
  , cec.cook_routine
  , cec.cook_routine_duration
  , cec.uses_broil  
  , cec.uses_steam
  , cec.cook_event_subtype AS recipe_type 
  , m.id IS NOT NULL AS is_recipe_card 
FROM {{ ref('cook_events') }} cec 
LEFT JOIN {{ table_reference('meals') }} m 
  ON {{ meal_sku_id('cec.barcode') }} = m.id
LEFT JOIN {{ ref('tovala_assist_barcodes') }} tab
  ON m.title = tab.title
WHERE cec.cook_event_subtype IN ('tovala_assist_recipe', 'tovala_preset_recipe')
