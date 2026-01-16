{{
  config(
    tags=["retool"]
  ) 
}}

-- Used by: Demand Planning/Aggregated Oven Sales 
-- Used by: Demand Planning/Oven Sales by Facility Network

WITH oven_sales AS (
  SELECT 
    CONVERT_TIMEZONE('America/Chicago', oo.order_time)::DATE AS sale_date_chicago
    , oo.first_orderable_term_id
    , os.destination_zip_cd
    , {{ facility_network('uss.facility_network') }} AS facility_network
    , COUNT(DISTINCT oo.oven_order_id) AS oven_sales
  FROM {{ ref('oven_orders') }} oo
  LEFT JOIN {{ ref('oven_shipments') }} os
    ON oo.oven_order_id = os.oven_order_id
  LEFT JOIN {{ ref('upcoming_subterm_shipping')}} uss
    ON os.destination_zip_cd = uss.zip_cd
    AND uss.term_id = {{ live_term(availability_check=True) }}
    AND uss.cycle = 1
  WHERE oo.status = 'complete'
    AND NOT oo.is_free_oven
  GROUP BY 1,2,3,4
)
SELECT 
  first_orderable_term_id
  , facility_network
  , sale_date_chicago
  , SUM(oven_sales) AS oven_sales
FROM oven_sales
WHERE first_orderable_term_id BETWEEN (SELECT MIN(term_id) FROM {{ ref('terms') }} WHERE order_by_time >= DATEADD('month', -15, CURRENT_DATE())) 
                              AND {{ live_term(availability_check=True) }}
GROUP BY 1,2,3
