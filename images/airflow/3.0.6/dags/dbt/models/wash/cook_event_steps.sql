{{ 
    config(
        materialized='incremental',
        unique_key='step_id',
        tags=["thyme_incremental"]
    ) 
}}

SELECT
  -- NOTE: all values for settings exclude test and debug barcodes
  {{ hash_natural_key('cs.unique_cook_cycle_id', 'index::STRING') }} AS step_id
  , cs.cookcycleid AS cook_cycle_id
  , cs.unique_cook_cycle_id 
  , oe.updated
  , oe.oven_generation
  , index+1 AS step_order
  , value AS raw_step_json
  -- SETTINGS: bottom, broil, fan, top, and steam
    -- bottom, fan, steam, and top are ALWAYS populated if mode is not NULL
    -- broil is occasionally NULL for earlier values
  -- bottom: 0 or 1
  , COALESCE(raw_step_json:bottom::INTEGER, raw_step_json:direct:bottom::INTEGER) AS bottom 
  -- broil: Usually 0 or .8, other values (.4, .6, and 1)
  , ROUND(raw_step_json:broil::FLOAT, 1) AS broil 
  -- fan: true, 0 (off), 1 (on), 2(more...on?), 3 (morer on?)L
  , COALESCE(raw_step_json:fan::INTEGER, raw_step_json:direct:fan::INTEGER) AS fan 
  -- steam: Usually 0 or .65 but other values (.25, .5, .6, .75, .8, 1) are possible for earlier cook events
  , ROUND(COALESCE(raw_step_json:steam::FLOAT, raw_step_json:direct:steam::FLOAT), 2) AS steam
  -- top: Usually 0 or 1 but other values (50, 100) are possible for earlier cook events
  , COALESCE(raw_step_json:top::INTEGER, raw_step_json:direct:top::INTEGER) AS top  
  , {{ clean_string('raw_step_json:mode::STRING') }} AS raw_mode 
  , CASE WHEN RLIKE(cs.barcode, '.*(TEST|DEBUG:).*', 'i')
         THEN 'test'
         -- If there is a mode, use the mode to determine event step type
         WHEN raw_mode IS NOT NULL 
         THEN CASE WHEN (oven_generation = 'airvala' AND raw_mode IN ('air_fry', 'airfry'))
                   THEN 'air_fry'
                   WHEN raw_mode IN ('bake', 'nofan_bake')
                   THEN 'bake'
                   WHEN raw_mode IN ('broil_Hi', 'broil_hi', 'broil_high')
                   THEN 'broil_high'
                   WHEN raw_mode IN ('broil_lo', 'broil_low')
                   THEN 'broil_low'
                   WHEN raw_mode IN ('conv_bake', 'convection_bake') OR 
                                    (oven_generation = 'gen_2' AND raw_mode IN ('air_fry', 'airfry'))
                   THEN 'convection_bake'
                   WHEN raw_mode = 'heat_ramp'
                   THEN 'heat_ramp'
                   WHEN raw_mode = 'heat_ramp_gentle'
                   THEN 'heat_ramp_gentle'
                   WHEN raw_mode = 'idle'
                   THEN 'idle'
                   WHEN raw_mode IN ('steam', 'convection_steam')
                   THEN 'steam'
             END
        -- Logic from firmware code: https://github.com/tovala/firmware-airvala/blob/master/src/system_controllers/presetChef.class.nut#L498
        WHEN steam <> 0
        THEN 'steam'
        WHEN COALESCE(broil, 0) > 0
        THEN 'broil_high'
        WHEN fan = 3 AND oven_generation = 'airvala'
        THEN 'air_fry'
        WHEN bottom = 0 and top = 1
        THEN 'broil_low'
        WHEN fan = 0
        THEN 'bake'
        ELSE 'convection_bake'
    END AS step_mode
  -- step_time: This can sometimes be negative. The sum of the steps is still equal to total cook time
  , COALESCE(raw_step_json:time, raw_step_json:cookTime) AS step_time 
  -- step_temperature: Sometimes this number is insanely high
  , raw_step_json:temperature::INTEGER AS step_temperature 
FROM {{ table_reference('oven_logs_cookingstarted') }} cs 
INNER JOIN {{ table_reference('oven_events', 'grind') }} oe 
  ON oe.oven_event_id = cs.oven_event_id,
  LATERAL FLATTEN(routine)
-- Only preset cook events for airvala or gen_2 will have routines
WHERE (cs.type IN ('preset', 'routine'))
  AND oe.oven_generation IN ('airvala', 'gen_2')
  AND cs.cookcycleid IS NOT NULL
  {% if is_incremental() %}
  AND cs.updated >= (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
