
SELECT 
    c.customer_id AS referrer_customer_id
    , cf.first_dtc_purchase_time
    , oo.customer_id AS referred_customer_id
    , oo.oven_order_id AS referred_oven_order_id
    , oo.order_time AS referred_oven_order_time
    , DATEDIFF('week',cf.first_dtc_purchase_time, oo.order_time) AS weeks_between_purchase_and_referral
    , ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY oo.order_time) AS referral_number
    , ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY oo.order_time DESC) = 1 AS is_most_recent_record
FROM {{ ref('customers') }} c
INNER JOIN {{ ref('customer_facts') }} cf
    ON c.customer_id = cf.customer_id
INNER JOIN {{ ref('oven_orders') }} oo
  ON c.referral_cd = oo.referral_cd
  AND oo.status IN ('complete','refunded')
