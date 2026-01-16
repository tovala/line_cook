-- Monthly summary of Oven Cost of Goods Sold (COGS) provided by Finance team 
-- This data is pulled from the Oven COGS per Month Input Table in Sigma 

SELECT 
    oven_generation 
    , id AS sigma_id 
    , DATEADD(day, 1, month::DATE) AS selected_date 
    , CONCAT(YEAR(selected_date), '-', MONTH(selected_date)) AS month 
    -- COGS value here refers to the average COGS for an oven in the given month 
    , cogs::FLOAT AS oven_cogs   
    , is_estimate::BOOLEAN  AS is_estimate
    , last_updated_at
    , last_updated_by
FROM {{ source('sigma_input_tables', 'oven_cogs_per_month') }} 
WHERE oven_generation IS NOT NULL 
AND month IS NOT NULL 
-- Pull the most recently updated row in the input table 
QUALIFY ROW_NUMBER() OVER (PARTITION BY month, oven_generation ORDER BY last_updated_at DESC) = 1 