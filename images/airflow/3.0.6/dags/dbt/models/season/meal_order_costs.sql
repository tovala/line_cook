
WITH sum_meal_cost AS (
  SELECT
    ms.meal_order_id
    , SUM(msk.cogs) AS meal_cost
  FROM {{ ref('meal_selections') }} ms
  LEFT JOIN {{ ref('meal_skus') }} msK
    ON ms.meal_sku_id = msk.meal_sku_id
  GROUP BY 1
)
, term_labor_cost_filter AS (
  SELECT
    *
  FROM {{ source('retool', 'term_labor_costs') }} tlc
  QUALIFY ROW_NUMBER() OVER (PARTITION BY term_id ORDER BY data_entry_time DESC) = 1
)

SELECT
  mo.meal_order_id
  , smc.meal_cost
  , CASE WHEN mo.facility_network = 'chicago' 
         THEN tlc.chi_labor_cost_per_meal * mo.order_size
         WHEN mo.facility_network = 'slc' 
         THEN tlc.slc_labor_cost_per_meal * mo.order_size
    END AS labor_cost 
  , CASE WHEN sbt.stripe_type = 'charge' 
         THEN {{ cents_to_usd('sbt.fee') }}
    END AS cc_fee
FROM {{ ref( 'meal_orders' ) }} mo
LEFT JOIN sum_meal_cost smc
  ON mo.meal_order_id = smc.meal_order_id
LEFT JOIN {{ ref( 'payments' ) }} p
  ON mo.payment_id = p.payment_id
LEFT JOIN {{ table_reference( 'stripe_balance_transactions' ) }} sbt
  ON p.stripe_charge_id = sbt.source
LEFT JOIN term_labor_cost_filter tlc
  ON mo.term_id = tlc.term_id
WHERE mo.is_fulfilled
