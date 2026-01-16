{{ dry_config('menu_products', 
              primary_key = 'product_id', 
              pre_hooks=[], 
              post_hooks=["{{ remove_soft_deletes() }}"]
) }}

SELECT
  created
  , deleted
  , {{ clean_string('misevala_meal_version_id') }} AS misevala_meal_version_id
  , {{ clean_string('product_id') }} AS product_id
  , {{ clean_string('title') }} AS title
  , updated
FROM {{ source('combined_api_v3', 'menu_products') }}

{{ load_incrementally_soft_delete() }}
