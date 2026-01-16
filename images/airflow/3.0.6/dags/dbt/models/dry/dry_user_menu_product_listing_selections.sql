{{ dry_config('user_menu_product_listing_selections') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('listing_id') }} AS listing_id
  , {{ clean_string('mealselectionid') }} AS mealselectionid
  , {{ clean_string('menu_id') }} AS menu_id 
  , updated
  , user_id
FROM {{ source('combined_api_v3', 'user_menu_product_listing_selections') }}

{{ load_incrementally() }}
