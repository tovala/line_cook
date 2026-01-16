{{ dry_config('menu_product_listing_attachments') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('listing_id') }} AS listing_id
  , {{ clean_string('menu_id') }} AS menu_id
  , meal_code
  , updated
FROM {{ source('combined_api_v3', 'menu_product_listing_attachments') }}

{{ load_incrementally() }}
