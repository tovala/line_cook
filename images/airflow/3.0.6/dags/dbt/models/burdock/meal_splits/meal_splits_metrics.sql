{{
  config(
    tags=['meal_splits'],
  )
}}

SELECT
    run_timestamp
    , ROUND(TRY_TO_DOUBLE(metrics['rmse']::STRING), 3) AS rmse
    , ROUND(TRY_TO_DOUBLE(metrics['r2']::STRING), 3) AS r2_score
    , ROUND(TRY_TO_DOUBLE(metrics['mape']::STRING), 3) AS mape
FROM {{ ref('meal_splits_logic') }}
