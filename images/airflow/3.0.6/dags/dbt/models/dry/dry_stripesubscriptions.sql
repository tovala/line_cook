{{ dry_config('stripesubscriptions', primary_key='userid') }}

SELECT
  active
  , autofill_breakfast_ok
  , autofill_surcharge_ok
  , commitment
  , {{ clean_string('customerid') }} AS customerid
  , default_meal_count
  , double
  , do_not_replace
  , last_updated
  , {{ clean_string('typeid') }} AS typeid
  , updated
  , userid
  , default_ship_period
  , premium_meals_valid
  , has_blacksheet_tray
  , trusted_payment_source
FROM {{ source('combined_api_v3', 'stripesubscriptions') }}

{{ load_incrementally() }}
