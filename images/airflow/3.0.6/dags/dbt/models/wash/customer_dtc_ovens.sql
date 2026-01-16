WITH tv_oven_orders AS (
  SELECT
    customer_id
  FROM {{ ref('tv_oven_orders') }}
  GROUP BY customer_id
)

SELECT
  oo.customer_id
  , oo.oven_order_id
  , oo.order_time AS first_dtc_purchase_time
  , oo.oven_generation AS first_oven_generation
  , oo.referral_cd AS first_oven_referral_cd
  , dmo.oven_order_id IS NOT NULL AS is_direct_mail_oven
  , oo.is_affirm_order AS is_first_oven_affirm_order
  , oo.coupon_cd AS first_oven_coupon_cd
  , oo.oven_sale_price AS first_oven_sale_price
  , oo.oven_purchase_price AS first_oven_purchase_price
  , oo.oven_charge_amount AS first_oven_charge_amount
  , oo.is_commitment_purchase AS is_first_oven_commitment_purchase
  , oo.is_commitment_forgiven AS is_commitment_forgiven
  , oo.commitment_week_count AS first_oven_commitment_week_count
  , oo.first_orderable_term_id AS first_oven_first_orderable_term_id
  , COUNT(oo.customer_id) OVER (PARTITION BY oo.customer_id ORDER BY oo.order_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS dtc_ovens_purchase_count
  , tvo.customer_id IS NOT NULL AS is_spotlight_tv_customer
FROM {{ ref('oven_orders') }} oo
LEFT JOIN {{ ref('direct_mail_ovens') }} dmo
  ON oo.oven_order_id = dmo.oven_order_id
  AND dmo.mailer_variant = 'received_mailer'
LEFT JOIN tv_oven_orders tvo
  ON oo.customer_id = tvo.customer_id
WHERE status IN ('refunded', 'complete')
QUALIFY ROW_NUMBER() OVER (PARTITION BY oo.customer_id ORDER BY oo.order_time) = 1
