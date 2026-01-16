{{
  config(
    alias='pos_invoice_charges', 
    materialized='incremental', 
    unique_key='id' 
  ) 
}}

SELECT 
  {{ clean_string('id') }} AS id 
  , {{ clean_string('invoice_id') }} AS invoice_id
  , {{ clean_string('reference_id') }} AS reference_id
  , {{ clean_string('reference_type') }} AS reference_type
  , {{ clean_string('product_sku') }} AS product_sku
  , {{ clean_string('refunded_by') }} AS refunded_by
  , {{ clean_string('billing_code') }} AS billing_code
  , tax_rate 
  , {{ cents_to_usd('tax_cents') }} AS tax 
  , {{ cents_to_usd('amount_cents') }} AS amount 
  , {{ cents_to_usd('discount_cents') }} AS discount 
  , created
  , updated
FROM {{ source('orders_pos', 'invoice_charges') }}
{% if is_incremental() %}
WHERE updated >= (SELECT MAX(updated) FROM {{ this }}) 
{% endif %}