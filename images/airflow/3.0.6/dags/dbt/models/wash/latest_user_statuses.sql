
SELECT
    customer_id
    , status_id
    , status_start_time
    , subscription_type
    , default_order_size
    , wants_double_autofill
    , cycle
    , user_status
    , on_commitment
    , prev_user_status
    , prev_on_commitment
    , wants_breakfast_autofill
    , wants_surcharged_autofill
FROM {{ ref('full_user_statuses') }} fus 
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY status_start_time DESC) = 1
