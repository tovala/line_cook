{{ dry_config('order_discount', 
              tags = ['marketing_incentives']
            ) 
}}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('description') }} AS description
  , item_discount_amount
  , {{ clean_string('item_discount_type') }} AS item_discount_type
  , shipping_discount_amount
  , {{ clean_string('shipping_discount_type') }} AS shipping_discount_type
  , created
  , updated
FROM {{ source('combined_api_v3', 'order_discount') }}

{{ load_incrementally() }}
