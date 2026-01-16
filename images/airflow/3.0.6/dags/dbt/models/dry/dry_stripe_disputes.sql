{{
  config(
    alias='stripe_disputes',
    materialized='incremental', 
    unique_key='id'
  ) 
}}

SELECT 
  id 
  , reason 
  , amount 
  , TO_TIMESTAMP_TZ(created::VARCHAR) AS created   
  , status AS stripe_status
  , charge 
  , is_charge_refundable 
  , currency
  , TO_TIMESTAMP_TZ(updated::VARCHAR) AS updated
FROM {{ source('stripe', 'disputes') }}

{% if is_incremental() %}
WHERE TO_TIMESTAMP_TZ(updated::VARCHAR) >= (SELECT MAX(updated) FROM {{this}} )
{%- endif -%}
