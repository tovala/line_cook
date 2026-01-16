{{
  config(
    alias='pos_invoice_refunds', 
    materialized='incremental', 
    unique_key='refund_item_id' 
  ) 
}}

SELECT 
  {{ clean_string('refund_item_id') }} AS refund_item_id
  , {{ clean_string('refund_id') }} AS refund_id  
  , {{ clean_string('invoice_id') }} AS invoice_id
  , {{ clean_string('payment_line_id') }} AS payment_line_id
  , {{ clean_string('payment_source_ref_id') }} AS payment_source_ref_id
  , {{ clean_string('account_type') }} AS account_type
  , {{ cents_to_usd('amount_cents') }} AS amount 
  , created
FROM {{ source('orders_pos', 'invoice_refunds') }}
{% if is_incremental() %}
WHERE created >= (SELECT MAX(created) FROM {{ this }}) 
{% endif %}