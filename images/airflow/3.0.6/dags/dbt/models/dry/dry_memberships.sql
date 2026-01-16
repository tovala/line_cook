{{ dry_config('memberships') }}

SELECT
  created
  , {{ clean_string('id') }} AS id
  , {{ clean_string('membership_type_id') }} AS membership_type_id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , start_date
  , {{ clean_string('status') }} AS status
  , updated
  , userid
FROM {{ source('combined_api_v3', 'memberships') }}

{{ load_incrementally() }}
