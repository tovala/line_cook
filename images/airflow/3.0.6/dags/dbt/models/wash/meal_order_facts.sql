
WITH selection_counts AS (
  SELECT 
    sel.meal_order_id
    , COUNT(CASE WHEN sku.is_premium THEN sel.meal_selection_id END) AS premium_meal_count
    , COUNT(CASE WHEN sku.is_surcharged THEN sel.meal_selection_id END) AS surcharged_meal_count
    , COUNT(CASE WHEN sku.is_dual_serving THEN sel.meal_selection_id END) AS dual_serving_meal_count
    , COUNT(CASE WHEN sku.is_breakfast THEN sel.meal_selection_id END) AS breakfast_meal_count
    , COUNT(CASE WHEN sel.is_autoselection THEN sel.meal_selection_id END) AS autoselection_count
  FROM {{ ref('meal_selections') }} sel
  LEFT JOIN {{ ref('meal_skus') }} sku
    ON sel.meal_sku_id = sku.meal_sku_id
  WHERE sel.is_fulfilled
  GROUP BY 1
)
SELECT
  mo.meal_order_id 
  , mo.customer_id 
  -- Stripe-specific identifier for a specific card/bank account for fraud-detection purposes 
  , mo.stripe_fingerprint AS stripe_fingerprint_id 
  , mo.term_id 
  , mo.status AS order_status
  , mo.is_break_skip
  , mo.is_fulfilled 
  , mo.is_commitment_order
  , mo.is_gma_order OR mo.is_trial_order AS is_gma_or_trial_order
  , mo.cycle
  , mo.facility_network
  , mo.order_time 
  , mo.order_size
  , ra.meal_arr
  , ship.destination_zip_cd
  , COALESCE(sc.premium_meal_count, 0) AS premium_meal_count
  , COALESCE(sc.surcharged_meal_count, 0) AS surcharged_meal_count
  , COALESCE(sc.dual_serving_meal_count, 0) AS dual_serving_meal_count
  , COALESCE(sc.breakfast_meal_count, 0) AS breakfast_meal_count
  , COALESCE(sc.autoselection_count, 0) AS autoselection_count
FROM {{ ref('meal_orders') }} mo
LEFT JOIN {{ ref('revenue_aggregations') }} ra 
  ON mo.meal_order_id = ra.order_id 
LEFT JOIN {{ ref('meal_shipments') }} ship
  ON ship.meal_order_id = mo.meal_order_id
LEFT JOIN selection_counts sc 
  ON sc.meal_order_id = mo.meal_order_id
