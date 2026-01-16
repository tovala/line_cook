{{
  config(
    alias='pos_invoice_status', 
    materialized='incremental', 
    unique_key='invoice_id' 
  ) 
}}

SELECT 
  {{ clean_string('invoice_id') }} AS invoice_id
  , {{ clean_string('status') }} AS status
  , created
FROM {{ source('orders_pos', 'invoice_status') }}
{% if is_incremental() %}
WHERE created >= (SELECT MAX(created) FROM {{ this }}) 
{% endif %}