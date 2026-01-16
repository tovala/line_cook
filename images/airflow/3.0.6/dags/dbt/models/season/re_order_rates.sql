SELECT
  menu_meal_id
  , meal_sku_id
  , COUNT(DISTINCT CASE WHEN is_reorder THEN customer_menu_meal_id END) AS re_order_count
  , COUNT(DISTINCT customer_menu_meal_id) AS re_order_all_count
  , CASE WHEN re_order_all_count = 0 
         THEN NULL 
         ELSE re_order_count / re_order_all_count 
    END AS re_order_rate
FROM  {{ ref('re_order_statuses') }} 
GROUP BY 1,2
