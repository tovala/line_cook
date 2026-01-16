-- Automated D2C Tracker 
-- Daily KPIs for Marketing spend and oven orders 

WITH oven_orders AS (
    SELECT 
        CONVERT_TIMEZONE('America/Chicago', oo.order_time)::DATE AS order_date
        , COUNT(DISTINCT oo.oven_order_id) AS total_orders
        , COUNT_IF(oo.is_first_oven_purchase=TRUE) AS new_customer_orders
        , SUM(oo.oven_purchase_price) AS oven_revenue
        -- Finance enters monthly Oven COGS numbers via a Sigma input table 
        -- We take the average oven COGS for the month and extrapolate to all ovens sold per oven generation 
        , SUM(CASE WHEN oo.oven_generation='gen_2' THEN oven_cogs ELSE 0 END) AS gen_2_cogs
        , SUM(CASE WHEN oo.oven_generation='airvala' THEN oven_cogs ELSE 0 END) AS airvala_cogs
    FROM {{ ref('oven_orders') }} oo 
    LEFT JOIN {{ ref('oven_cogs_per_month') }} cogs
        ON YEAR(CONVERT_TIMEZONE('America/Chicago',oo.order_time))=YEAR(cogs.selected_date)
        AND MONTH(CONVERT_TIMEZONE('America/Chicago',oo.order_time))=MONTH(cogs.selected_date)
        AND oo.oven_generation = cogs.oven_generation
    WHERE status<>'canceled'
    GROUP BY ALL 
)
SELECT 
    c.calendar_date AS report_date 
    , o.total_orders AS total_oven_orders 
    , o.new_customer_orders AS total_new_customer_orders
    , ROUND(total_new_customer_orders/total_oven_orders,2) AS pct_new_customers
    , ROUND(o.oven_revenue,2) AS total_oven_revenue 
    , ROUND(gen_2_cogs+airvala_cogs,2) AS total_oven_cogs 
    , ROUND(SUM(cs.daily_channel_spend),2) AS total_marketing_spend 
    , (total_oven_cogs - total_oven_revenue) AS total_oven_loss
    , (total_marketing_spend + total_oven_loss) AS total_cost 
    , ROUND((total_marketing_spend / total_oven_orders),2) AS media_cac 
    , ROUND((total_cost / total_oven_orders),2) AS nbCAC 
FROM {{ source('brine', 'dim_calendar') }} c
LEFT JOIN {{ ref('channel_spend') }} cs
    ON c.calendar_date=cs.spend_date 
LEFT JOIN oven_orders o 
    ON c.calendar_date=o.order_date 
WHERE cs.spend_date IS NOT NULL 
AND c.calendar_date<CURRENT_DATE() 
GROUP BY ALL 