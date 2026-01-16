{{ dry_config('meal_cashmoney') }}

SELECT
  {{ clean_string('action') }} AS action
  , amount
  , {{ clean_string('coupon_code') }} AS coupon_code
  , created
  , PARSE_JSON(cs_affected_listing_ids)::ARRAY AS cs_affected_listing_ids
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('purchase_id') }} AS purchase_id
  , {{ clean_string('type') }} AS type
  , updated
  , userid
  , {{ clean_string('cs_reason') }} AS cs_reason
  , {{ clean_string('cs_zd_ticket') }} AS cs_zd_ticket
  , {{ clean_string('cs_agent') }} AS cs_agent
  , PARSE_JSON(cs_affected_mealids)::ARRAY AS cs_affected_mealids
  , voided
FROM {{ source('combined_api_v3', 'meal_cashmoney') }}

{{ load_incrementally() }}