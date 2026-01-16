{{
  config(
    alias='stripe_balance_transactions',
    materialized='incremental', 
    unique_key='id'
  ) 
}}

SELECT 
  id
  , amount
  , TO_TIMESTAMP_TZ(available_on::VARCHAR) AS available_on
  , TO_TIMESTAMP_TZ(created::VARCHAR) AS created 
  , currency
  , description AS stripe_description
  , fee
  , net
  , source
  , status AS stripe_status
  , type AS stripe_type
FROM {{ source('stripe', 'balance_transactions') }}

{% if is_incremental() %}
WHERE TO_TIMESTAMP_TZ(created::VARCHAR) >= (SELECT MAX(created) FROM {{this}} )
{%- endif -%}