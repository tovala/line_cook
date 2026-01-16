{{ 
    config(
        materialized='incremental',
        unique_key='historic_affinity_id',
        tags=["thyme_incremental"]
    ) 
}}

SELECT 
  {{ hash_natural_key('raw_data', 'filename')}} AS historic_affinity_id
  , AS_INTEGER(raw_data:CUSTOMER_ID) AS customer_id
  , AS_INTEGER(raw_data:MEAL_AFFINITY) AS meal_affinity
  , AS_INTEGER(raw_data:MEAL_SKU_ID) AS meal_sku_id
  , AS_INTEGER(raw_data:TERM_ID) AS term_id
  , upload_time
  , DAYNAME(upload_time) AS snapshot_dow
  , DENSE_RANK() OVER (PARTITION BY upload_time::DATE ORDER BY upload_time) AS nth_snapshot_of_day
  , snapshot_dow = 'Thu' AND nth_snapshot_of_day = 1 AS is_autofill_affinity
  , updated
FROM {{ source('chili', 'table_snapshots') }}
WHERE table_name = 'customer_meal_affinity'
  {% if is_incremental() %}
  AND updated > (SELECT MAX(updated) FROM {{ this }})
  {% endif %}
