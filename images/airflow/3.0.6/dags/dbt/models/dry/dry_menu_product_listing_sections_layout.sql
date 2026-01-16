{{ dry_config('menu_product_listing_sections_layout') }}

SELECT
  created
  , display_position
  , {{ clean_string('id') }} AS id
  , {{ clean_string('listing_id') }} AS listing_id
  , {{ clean_string('section_id') }} AS section_id 
  , updated
FROM {{ source('combined_api_v3', 'menu_product_listing_sections_layout') }}

{{ load_incrementally() }}