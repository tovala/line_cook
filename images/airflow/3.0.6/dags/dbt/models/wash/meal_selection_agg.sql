-- This table fetches data for each term, each active customer(has a fulfilled order), count of meal selections for each meals on the menu. Also the total meal_selections.

 WITH matrix AS(
-- creating a empty matrx each term, each active customer,and each meal on menu.
  SELECT
    mo.term_id
    , mo.customer_id
    , mmo.menu_meal_id
    , TRY_TO_NUMERIC(mmo.meal_sku_id)::INTEGER AS meal_sku_id
    , mmo.internal_meal_id
    , mo.facility_network
    , mo.cycle
  FROM {{ ref('meal_orders') }} mo
  INNER JOIN {{ ref('menu_meal_offerings') }} mmo
    ON mo.subterm_id = mmo.subterm_id
  WHERE is_fulfilled
), meal_selection_sum AS (
-- Sum of meal selections for each meal(non_autofilled and fulfilled) 
  SELECT
   customer_id
   ,menu_meal_id
   ,meal_sku_id
   , COUNT(CASE WHEN is_autoselection = FALSE THEN meal_selection_id END) AS meal_selection_count
   , COUNT(DISTINCT meal_selection_id) AS meal_fulfilled_count
  FROM {{ ref('meal_selections') }}
  WHERE is_fulfilled
  GROUP BY 1,2,3
), total_count AS(
-- Sum of meal selections for each order (non_autofilled and fulfilled) 
  SELECT 
    customer_id
    , term_id
    , COUNT(CASE WHEN is_autoselection = FALSE THEN meal_selection_id END) AS selection_total_count
    , COUNT(DISTINCT meal_selection_id) AS meal_fulfilled_total_count
  FROM {{ ref('meal_selections') }}
  WHERE is_fulfilled
  GROUP BY 1,2
)
-- Join them together. With customer_id in this table we can slice the data base on customer info for downstream calc.
SELECT
  m.term_id
  , m.customer_id
  , m.facility_network 
  , m.cycle
  , m.menu_meal_id
  , m.meal_sku_id
  , m.internal_meal_id
  , COALESCE(mss.meal_selection_count, 0) AS meal_selection_count
  , COALESCE(mss.meal_fulfilled_count, 0) AS meal_fulfilled_count
  , COALESCE(tc.selection_total_count, 0) AS selection_total_count
  , COALESCE(tc.meal_fulfilled_total_count, 0) AS meal_fulfilled_total_count
FROM matrix m
LEFT JOIN meal_selection_sum mss
  ON m.customer_id = mss.customer_id
  AND m.menu_meal_id = mss.menu_meal_id
LEFT JOIN total_count tc
  ON m.customer_id = tc.customer_id
  AND m.term_id = tc.term_id
