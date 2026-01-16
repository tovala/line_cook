{{ dry_config('menu_product_listings', 
              primary_key = 'listing_id', 
              pre_hooks=[], 
              post_hooks=["{{ remove_soft_deletes() }}"]
) }}

SELECT
  created
  , deleted
  , expiration_date
  , {{ clean_string('listing_id') }} AS listing_id
  , price_cents
  , {{ clean_string('product_id') }} AS product_id
  , sold_out_count
  , updated
  , badge_text
FROM {{ source('combined_api_v3', 'menu_product_listings') }}

{{ load_incrementally_soft_delete() }}
