{{ 
    config(
        materialized='incremental',
        unique_key='unique_cook_cycle_id',
        tags=["thyme_incremental"]
    ) 
}}

WITH numbered_temperatures AS (
  SELECT 
    cook_cycle_id
    , unique_cook_cycle_id
    , temperature_time
    , chamber_temperature 
    , RANK() OVER (PARTITION BY unique_cook_cycle_id ORDER BY temperature_time) = 1 AS first_row
    , RANK() OVER (PARTITION BY unique_cook_cycle_id ORDER BY temperature_time DESC) = 1 AS last_row
    , DENSE_RANK() OVER (PARTITION BY unique_cook_cycle_id ORDER BY chamber_temperature DESC) = 1 AS is_maximum
  FROM {{ table_reference('cook_event_temperatures', 'season') }}
  WHERE chamber_temperature IS NOT NULL
    {% if is_incremental() %}
    AND temperature_time >= (SELECT MAX(temperature_start_time) FROM {{this}} )
    {% endif %}
  QUALIFY last_row OR first_row OR is_maximum
)
SELECT 
  cook_cycle_id 
  , unique_cook_cycle_id
  , MIN(temperature_time) AS temperature_start_time
  -- It is possible for there to be multiple temperatures at the same time so we average them
  , AVG(CASE WHEN first_row THEN chamber_temperature END) AS first_chamber_temperature
  , AVG(CASE WHEN last_row THEN chamber_temperature END) AS last_chamber_temperature
  , MAX(CASE WHEN is_maximum THEN chamber_temperature END) AS maximum_chamber_temperature
FROM numbered_temperatures
GROUP BY 1,2
