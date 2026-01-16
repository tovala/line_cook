{{
  config(
    alias='pos_invoices', 
    materialized='incremental', 
    unique_key='id' 
  ) 
}}

SELECT 
  {{ clean_string('id') }} AS id 
  , {{ clean_string('order_id') }} AS order_id
  , created
  , updated
FROM {{ source('orders_pos', 'invoices') }}
{% if is_incremental() %}
WHERE updated >= (SELECT MAX(updated) FROM {{ this }}) 
{% endif %}