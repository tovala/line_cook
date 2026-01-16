{{
  config(
    alias='pos_invoice_payments', 
    materialized='incremental', 
    unique_key='payment_line_id' 
  ) 
}}

SELECT 
  {{ clean_string('payment_line_id') }} AS payment_line_id
  , {{ clean_string('id') }} AS id  
  , {{ clean_string('invoice_id') }} AS invoice_id
  , {{ clean_string('payment_source_ref_id') }} AS payment_source_ref_id
  , {{ clean_string('account_type') }} AS account_type
  , {{ cents_to_usd('amount_cents') }} AS amount 
  , created
FROM {{ source('orders_pos', 'invoice_payments') }}
{% if is_incremental() %}
WHERE created >= (SELECT MAX(created) FROM {{ this }}) 
{% endif %}