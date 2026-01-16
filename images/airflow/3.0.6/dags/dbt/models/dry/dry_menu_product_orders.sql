
{{ dry_config('menu_product_orders') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
FROM {{ source('combined_api_v3', 'menu_product_orders') }}

{{ load_incrementally(bookmark='created') }}
