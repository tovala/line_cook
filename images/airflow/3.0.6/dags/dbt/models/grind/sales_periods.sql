SELECT
  start_date
  , end_date
  , period_type
  , sale_period
  , {{ price_to_decimal('airvala_oven_price') }} AS airvala_oven_price
  , {{ price_to_decimal('gen_2_oven_price') }} AS gen_2_oven_price
  , agg_oven_price
  , commitment_weeks
  , {{ price_to_decimal('referral_airvala_price') }} AS referral_airvala_price
  , {{ price_to_decimal('referral_gen_2_price') }} AS referral_gen_2_price
  , referral_oven_price
  , CAST(referral_commitment_weeks AS INTEGER) AS referral_commitment_weeks
FROM {{ source('sigma_input_tables', 'sales_period_output') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY start_date ORDER BY last_updated_at DESC) = 1