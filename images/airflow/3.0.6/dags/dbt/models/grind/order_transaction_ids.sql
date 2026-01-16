SELECT 
  orders_pos_id AS point_of_sale_id
  , orders_pos_invoice_id AS invoice_id
  , source_order_id AS order_id
  , CASE WHEN source_order_type IN ('tovala-oven', 'tovala-gift-card', 'tovala-commitment-release')
         THEN REGEXP_REPLACE(source_order_type, 'tovala-', '')
    END AS order_type 
  , created AS invoice_time
FROM {{ table_reference('orders_pos_mappings') }}
