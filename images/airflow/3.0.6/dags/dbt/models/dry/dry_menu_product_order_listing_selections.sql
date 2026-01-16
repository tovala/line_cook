
{{ dry_config('menu_product_order_listing_selections') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('listing_id') }} AS listing_id
  , {{ clean_string('mealselectionid') }} AS mealselectionid
  , {{ clean_string('menu_id') }} AS menu_id
  , {{ clean_string('menu_product_order_id') }} AS menu_product_order_id
  , refunded
  , updated
FROM {{ source('combined_api_v3', 'menu_product_order_listing_selections') }}

{{ load_incrementally() }}
