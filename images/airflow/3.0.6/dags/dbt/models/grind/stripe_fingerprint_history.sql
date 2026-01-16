SELECT
    id AS status_id
    , userid AS customer_id
    , customerid AS stripe_customer_id 
    , fingerprint AS stripe_fingerprint 
    , paymentsourceid AS stripe_payment_source_id
    , created AS status_start_time
    , LEAD(created) OVER (PARTITION BY userid ORDER BY created) AS status_end_time
    , status_end_time IS NULL AS is_current_fingerprint
FROM {{ table_reference('stripefingerprints') }}
