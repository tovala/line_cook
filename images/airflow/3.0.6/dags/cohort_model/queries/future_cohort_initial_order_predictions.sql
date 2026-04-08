CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".future_cohort_initial_order_predictions
AS
WITH sales_splits AS (
  SELECT 
    COALESCE(cohort_id, term_id) AS cohort
    ,SUM(
      CASE 
        WHEN is_sale_period AND NOT is_holiday THEN projected_d2c_sales 
        ELSE 0 
      END) AS sale_d2c_total_sales
    ,SUM(
      CASE 
        WHEN is_holiday AND NOT is_sale_period THEN projected_d2c_sales
        ELSE 0
      END) AS holiday_d2c_total_sales
    ,SUM(
      CASE 
        WHEN NOT is_sale_period AND NOT is_holiday THEN projected_d2c_sales
        ELSE 0
      END) AS non_holiday_d2c_total_sales
    ,SUM(projected_amazon_sales) AS amazon_total_sales
      FROM mugwort.combined_oven_sales GROUP BY cohort ORDER BY cohort
),
six_week_vectors AS (
  SELECT 
    cohort
    ,ARRAY_REVERSE(
        ARRAY_AGG(sale_d2c_total_sales) OVER
        (ORDER BY cohort
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    ) AS sale_d2c_prev_6_attach_weeks
    ,ARRAY_REVERSE(
        ARRAY_AGG(holiday_d2c_total_sales) OVER  
        (ORDER BY cohort
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    ) AS holiday_d2c_prev_6_attach_weeks
    ,ARRAY_REVERSE(
        ARRAY_AGG(non_holiday_d2c_total_sales) OVER
        (ORDER BY cohort
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    ) AS non_holiday_d2c_prev_6_attach_weeks
    ,ARRAY_REVERSE(
      ARRAY_AGG(amazon_total_sales) OVER
      (ORDER BY cohort
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
    ) AS amazon_prev_6_attach_weeks
  FROM sales_splits ORDER BY cohort
),
init_order_split AS (
  SELECT 
    cohort
    ,VECTOR_INNER_PRODUCT(
      sale_d2c_prev_6_attach_weeks::VECTOR(FLOAT, 6),
      {{ params.six_week_attach_rates_sale }}::VECTOR(FLOAT, 6)
    ) as init_sale_order_count
    ,VECTOR_INNER_PRODUCT(
      holiday_d2c_prev_6_attach_weeks::VECTOR(FLOAT, 6),
      {{ params.six_week_attach_rates_d2c_holiday }}::VECTOR(FLOAT, 6)
    ) as init_holiday_order_count
    ,VECTOR_INNER_PRODUCT(
      non_holiday_d2c_prev_6_attach_weeks::VECTOR(FLOAT, 6),
      {{ params.six_week_attach_rates_d2c_non_holiday }}::VECTOR(FLOAT, 6)
    ) as init_non_holiday_order_count
    ,VECTOR_INNER_PRODUCT(
      amazon_prev_6_attach_weeks::VECTOR(FLOAT, 6),
      {{ params.six_week_attach_rates_amazon }}::VECTOR(FLOAT, 6)
    ) as init_amazon_order_count
  FROM six_week_vectors WHERE cohort >= (SELECT term_id FROM grind.terms WHERE is_live)
)
SELECT
  cohort
  ,(init_sale_order_count + init_holiday_order_count + init_non_holiday_order_count + init_amazon_order_count) as init_order_val
FROM init_order_split;