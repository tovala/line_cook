-- Linear TV - Forecast Labs (formerly known as Spotlight TV) orders
-- Should exclude any data groups that are also being run through TVSquared
-- Currently, Ralph uploads all Spotlight TV orders in Sigma via the Marketing Channel Spend Input dashboard
-- This table will only contain data from September 2025 onwards 

SELECT 
    id AS sigma_id
    , 'Linear TV - Forecast Labs' AS channel_name
    , 'Forecast Labs' AS partner
    , date AS spend_date  
    , ROUND(orders,2) AS total_orders 
    -- Contract with Spotlight TV sets spend per order at $62.50
    , total_orders*62.5 AS total_spend 
    -- Set data source and update cadence to indicate this data is pulled from Sigma 
    , 'sigma_input_table' AS data_source 
    , 'manual_input' AS update_cadence_type
    , last_updated_at 
    , last_updated_by
FROM {{ source('sigma_input_tables', 'spotlight_tv_orders') }} 
WHERE spend_date IS NOT NULL 
-- Pull the most recently updated row in the input table 
QUALIFY ROW_NUMBER() OVER (PARTITION BY spend_date ORDER BY last_updated_at DESC) = 1 