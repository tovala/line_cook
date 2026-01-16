{{ dry_config('sts_products') }}

SELECT
  TRY_PARSE_JSON(alternate_routines) AS alternate_routines
  , {{ clean_string('barcode') }} AS barcode
  , {{ clean_string('brand') }} AS brand
  , {{ clean_string('chef_notes') }} AS chef_notes
  , {{ clean_string('cohort') }} AS cohort
  , {{ clean_string('company') }} AS company
  , {{ clean_string('cookable_product_id') }} AS cookable_product_id
  , created
  , created_by_user_id
  , customer_visible
  , {{ clean_string('food_packaging_material') }} AS food_packaging_material
  , has_oven_instructions
  , {{ clean_string('id') }} AS id
  , TRY_PARSE_JSON(images) AS images
  , TRY_PARSE_JSON(mid_cycle_steps) AS mid_cycle_steps
  , mid_cycle_time_seconds
  , {{ clean_string('name') }} AS name
  , TRY_PARSE_JSON(post_cook_steps) AS post_cook_steps
  , TRY_PARSE_JSON(pre_cook_steps) AS pre_cook_steps
  , {{ clean_string('primary_category') }} AS primary_category
  , procurement_support
  , TRY_PARSE_JSON(routine) AS routine
  , TRY_PARSE_JSON(tags) AS tags
  , updated
  , updated_by_user_id
  , {{ clean_string('version') }} AS version
FROM {{ source('combined_api_v3', 'sts_products') }}

{{ load_incrementally() }}
