{{
  config(
    tags=['meal_splits'],
  )
}}

SELECT
    run_timestamp
    , meal_sku_id
    , predicted_split
FROM {{ ref('meal_splits_logic') }}
