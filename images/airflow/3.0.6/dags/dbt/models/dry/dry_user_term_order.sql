{{ dry_config('user_term_order') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , meal_count
  , {{ clean_string('menu_product_order_id') }} AS menu_product_order_id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('orderfulfillment_id') }} AS orderfulfillment_id
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , {{ clean_string('status') }} AS status
  , {{ clean_string('subscriptiontypes_id') }} AS subscriptiontypes_id
  , termid
  , updated
  , userid
  , default_meal_count
  , {{ clean_string('subterm_id') }} AS subterm_id
  , will_reprocess
FROM {{ source('combined_api_v3', 'user_term_order') }}

{{ load_incrementally() }}
