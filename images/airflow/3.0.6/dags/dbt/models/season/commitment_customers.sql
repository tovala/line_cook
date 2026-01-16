WITH total_associated_orders AS (
  SELECT 
    cf.customer_id 
    , COUNT(DISTINCT mo1.meal_order_id) + COUNT(DISTINCT mo2.meal_order_id) AS total_fulfilled_orders_count
  FROM {{ ref('customer_facts') }} cf 
  LEFT JOIN {{ ref('stripe_fingerprint_history') }} sh 
    ON cf.customer_id = sh.customer_id
  LEFT JOIN {{ ref('meal_orders') }} mo1
    ON COALESCE(mo1.stripe_fingerprint, '') = sh.stripe_fingerprint 
    AND mo1.is_fulfilled
  LEFT JOIN {{ ref('meal_orders') }} mo2
    ON mo2.stripe_fingerprint IS NULL 
    AND cf.customer_id = mo2.customer_id
    AND mo2.is_fulfilled
  GROUP BY ALL
),

informed_customers AS (
  (SELECT
     DISTINCT customer_id
   FROM {{ source('brine', 'commitment_enforcement_customers') }})
),

updated_sigma_status AS (
  SELECT
    customer_id,
    commitment_status
  FROM {{ source('sigma_input_tables', 'commitment_enforcement_entry')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_updated_at DESC) = 1
)

SELECT 
    cf.customer_id 
    , cf.first_oven_order_id 
    , cf.first_oven_commitment_week_count
    , tao.total_fulfilled_orders_count
    , GREATEST(cf.first_oven_commitment_week_count - tao.total_fulfilled_orders_count, 0) AS commitment_orders_remaining
    , CASE
         WHEN cf.is_commitment_forgiven
         THEN 'forgiven'
         WHEN commitment_orders_remaining = 0
         THEN 'complete'
         WHEN uss.commitment_status = 'charge_successful'
         THEN 'charge_successful'
         WHEN uss.commitment_status = 'charge_failed'
         THEN 'charge_failed'
         WHEN uss.commitment_status = 'sent_to_collections'
         THEN 'sent_to_collections'
         WHEN ic.customer_id IS NOT NULL
         THEN 'informed'
         WHEN commitment_orders_remaining > 0 
         -- These are all customers who haven't completed their commitment, but their eligibility for enforcement depends on how long since their oven purchase
         THEN CASE WHEN cf.first_dtc_purchase_time <= DATEADD(MONTH, -12, {{ current_timestamp_utc() }}) 
                        AND c.on_commitment
                   THEN 'past_enforcement'
                   WHEN cf.first_dtc_purchase_time > DATEADD(MONTH, -12, {{ current_timestamp_utc() }}) 
                        AND cf.first_dtc_purchase_time <= DATEADD(MONTH, -6, {{ current_timestamp_utc() }})
                        AND c.on_commitment
                   THEN 'enforcement_eligible'
                   WHEN cf.first_dtc_purchase_time > DATEADD(MONTH, -6, {{ current_timestamp_utc() }})
                        AND c.on_commitment
                   THEN 'within_commitment'
                   ELSE 'oven_refunded'
              END
      END AS commitment_status
    , cf.first_dtc_purchase_time
FROM {{ ref('customer_facts') }} as cf
LEFT JOIN {{ ref('customers') }} AS c
    ON cf.customer_id = c.customer_id
LEFT JOIN total_associated_orders tao 
    ON cf.customer_id = tao.customer_id
LEFT JOIN informed_customers as ic
    ON cf.customer_id = ic.customer_id
LEFT JOIN updated_sigma_status uss
    ON cf.customer_id = uss.customer_id
WHERE cf.is_first_oven_commitment_purchase
  -- in 2018/2019, we offered commitment on a meal basis rather than on a week basis so they are not applicable here 
  AND cf.first_oven_commitment_week_count > 0
  AND NOT cf.is_internal_account