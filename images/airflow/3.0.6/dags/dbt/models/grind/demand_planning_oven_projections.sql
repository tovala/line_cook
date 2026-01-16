
SELECT 
  TRY_TO_DATE({{ clean_string('projection_date') }}) AS projection_date
  , {{ clean_string('day_of_week') }} AS day_of_week 
  , TRY_TO_NUMERIC({{ clean_string('oven_sales_projections') }}) AS projected_oven_sales 
  , TRY_TO_NUMERIC({{ clean_string('oven_sales_actuals') }}) AS actual_oven_sales
  , TRY_TO_NUMERIC(REGEXP_REPLACE({{ clean_string('meal_term') }}, 'T', '')) AS meal_term
  , _airbyte_extracted_at AS updated_time
FROM {{ source('growth_team_daily_sales_projections', 'daily_oven_sales_projections') }}
