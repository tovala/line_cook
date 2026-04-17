CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".future_cohort_initial_order_predictions
AS
-- get the sum of orders on a per-cohort basis that fall into each of the different sales categories
WITH sales_splits AS (
  SELECT 
    COALESCE(cohort_id, term_id) AS cohort
    ,SUM(
      CASE 
        WHEN is_sale_period AND NOT is_holiday 
        THEN projected_d2c_sales 
        ELSE 0 
      END) AS sale_d2c_total_sales -- for each day of sales in that week, add the daily d2c sales prediction to the sum of d2c sale orders for week when the is_sale_period flag is set
    ,SUM(
      CASE 
        WHEN is_holiday AND NOT is_sale_period
        THEN projected_d2c_sales
        ELSE 0
      END) AS holiday_d2c_total_sales -- for each day of sales in that week, add the daily d2c sales prediction to the sum of d2c holiday orders for week when the is_holiday flag is set
    ,SUM(
      CASE 
        WHEN NOT is_sale_period AND NOT is_holiday
        THEN projected_d2c_sales
        ELSE 0
      END) AS non_holiday_d2c_total_sales -- for each day of sales in that week, add the daily d2c sales prediction to the sum of non-holiday d2c orders when no special flags are set
    ,SUM(projected_amazon_sales) AS amazon_total_sales -- sum all amazon daily sales predictions for the week
      FROM mugwort.combined_oven_sales GROUP BY cohort ORDER BY cohort
),
-- since cohort is based on the first term when a customer orders meals (and not on when they purchased the oven),
-- need to calculate first order term using the weekly sales predictions split by order type and six-week attach rates.
-- six_week_vectors = for each future cohort, retrieve the last six weeks of oven orders (split by type) ordered by most recent
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
-- to calculate future initial order count for a cohort, calculate Vector inner product on 
-- (6 previous weeks of orders) X (six week attach rates) for each order type
-- E.g. for Cohort X, say the predicted non_holiday d2c weekly sales are 
-- v_s = [200, 0, 20, 50, 0, 100] 
-- (meaning 200 non_holiday oven purchases were made the week of cohort, 0 non_holiday oven purchases were made one week prior, 20 non_holiday oven purchases were made 2 weeks prior, ...)
-- 
-- and six week attach rates for non_holiday sales are
-- v_r = [0.60, 0.15, 0.05, 0.10, 0.04, 0.01]
-- (meaning 60% of non_holiday sales order first meals the 1st week available after oven purchase, 15% order first meals the 2nd week after oven purchase, 5% order first meals the 3rd week after oven purchase, ...)
--
-- To find the total initial non_holiday d2c oven first meal orders for Cohort X (I(X)),
-- use Vector multiplication (dot product):
-- I(X) = v_s * v_r = 200*0.60 + 0*0.15 + 20*0.05 + 50*0.10 + 0*0.04 + 100*0.01 = 127
--
-- meaning we expect 127 initial meal orders to come from non_holiday oven purchases for cohort X
-- 
-- repeat this for each type of oven sales
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
  FROM six_week_vectors
  WHERE cohort >= (SELECT term_id FROM grind.terms WHERE is_live)
)
-- **TOTAL PREDICTED INITIAL ORDERS FOR A COHORT** is the Sum( v_s * v_r for [non_holiday_d2c, sale_d2c, holiday_d2c, amazon])
SELECT
  cohort
  ,(init_sale_order_count + init_holiday_order_count + init_non_holiday_order_count + init_amazon_order_count) as init_order_val
FROM init_order_split;