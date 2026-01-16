{{
  config(
    alias='pos_orders', 
    materialized='incremental', 
    unique_key='id' 
  ) 
}}

SELECT 
  {{ clean_string('id') }} AS id 
  , userid AS user_id
  , {{ clean_string('customer_name') }} AS customer_name
  , {{ clean_string('customer_email') }} AS customer_email
  , {{ clean_string('shipping_address_name') }} AS shipping_address_name  
  , {{ clean_string('shipping_address_phone') }} AS shipping_address_phone
  , {{ clean_string('shipping_address_zip') }} AS shipping_address_zip
  , {{ clean_string('shipping_address_state') }} AS shipping_address_state
  , {{ clean_string('shipping_address_city') }} AS shipping_address_city
  , {{ clean_string('shipping_address_line1') }} AS shipping_address_line1
  , {{ clean_string('shipping_address_line2') }} AS shipping_address_line2
  , created
FROM {{ source('orders_pos', 'orders') }}
{% if is_incremental() %}
WHERE created >= (SELECT MAX(created) FROM {{ this }}) 
{% endif %}