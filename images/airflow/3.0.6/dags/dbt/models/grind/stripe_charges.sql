{{ 
    config(
        materialized='incremental',
        tags=["thyme_incremental"],
        unique_key='stripe_charge_id'
    ) 
}}

SELECT
  id AS stripe_charge_id
  , customer AS stripe_customer_id
  , status
  , paid AS is_paid
  , object AS transaction_type
  , statement_descriptor
  , TO_TIMESTAMP_TZ(created) AS created_time
  , TO_TIMESTAMP_TZ(updated) AS updated_time
  , CASE WHEN TRY_PARSE_JSON(outcome:rule) IS NULL
         THEN outcome:rule::VARCHAR
         ELSE TRY_PARSE_JSON(outcome:rule):id::VARCHAR
    END AS rule_id
  , TRY_PARSE_JSON(outcome:rule):action::VARCHAR AS rule_action
  , TRY_PARSE_JSON(outcome:rule):predicate::VARCHAR AS rule_condition
  , UPPER(payment_method_details:"card":"issuer")::VARCHAR AS card_issuer
  , UPPER(payment_method_details:"card":"brand")::VARCHAR AS card_brand
  , UPPER(payment_method_details:"card":"description")::VARCHAR AS card_description
  , UPPER(payment_method_details:"card":"funding")::VARCHAR AS card_funding
  , metadata:paymentID::VARCHAR AS payment_id
  , TRY_TO_NUMERIC(metadata:customer_id::VARCHAR)::INTEGER AS customer_id
FROM {{ source('stripe', 'charges') }}
{% if is_incremental() %}
WHERE TO_TIMESTAMP_TZ(updated) >= (SELECT MAX(updated_time) FROM {{ this }})
{% endif %}
