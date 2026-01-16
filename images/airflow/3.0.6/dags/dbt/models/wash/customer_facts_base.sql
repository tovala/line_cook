--All DTC oven purchasers
(SELECT customer_id 
 FROM {{ ref('customer_dtc_ovens') }})
   UNION
--All customers with at least one fulfilled meal order
(SELECT customer_id 
 FROM {{ ref('first_meal_orders') }})
   UNION
--All customers who synced oven via Retail oven serial number
(SELECT DISTINCT customer_id 
 FROM {{ ref('historic_oven_registrations') }} 
 WHERE oven_retailer IS NOT NULL 
   AND NOT is_internal_account)
