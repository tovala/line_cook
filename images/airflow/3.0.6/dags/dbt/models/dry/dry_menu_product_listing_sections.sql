{{ dry_config('menu_product_listing_sections') }}

SELECT
  created
  , display_position
  , {{ clean_string('id') }} AS id
  , {{ clean_string('menu_id') }} AS menu_id
  , {{ clean_string('title') }} AS title
  , updated
FROM {{ source('combined_api_v3', 'menu_product_listing_sections') }}

{{ load_incrementally() }}