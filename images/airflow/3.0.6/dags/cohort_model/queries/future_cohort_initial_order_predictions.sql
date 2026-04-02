CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".future_cohort_initial_order_predictions
AS
WITH sales_splits AS (
SELECT 
coalesce(cohort_id, term_id) AS cohort
, sum(CASE WHEN is_sale_period AND NOT is_holiday THEN projected_d2c_sales ELSE 0 END) AS sale_d2c_total
, sum(CASE WHEN is_holiday AND NOT is_sale_period THEN projected_d2c_sales ELSE 0 END) AS holiday_d2c_total
, sum(CASE WHEN NOT is_sale_period AND NOT is_holiday THEN projected_d2c_sales ELSE 0 END) AS non_holiday_d2c_total
, sum(projected_d2c_sales) AS all_d2c_total
, sum(projected_amazon_sales) AS amazon_total
, sum(projected_costco_sales) AS costco_total
FROM mugwort.combined_oven_sales GROUP BY cohort ORDER BY cohort)
SELECT 
cohort
,(sale_d2c_total*0.704+holiday_d2c_total*0.704+non_holiday_d2c_total*0.721+amazon_total*0.000+costco_total*0.000)::INTEGER AS predicted_initial_order_count
FROM sales_splits;