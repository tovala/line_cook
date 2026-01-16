{{ dry_config('subscriptionactivity') }}

SELECT
  active
  , autofill_breakfast_ok
  , autofill_surcharge_ok
  , commitment
  , created
  , {{ clean_string('customerid') }} AS customerid
  , default_meal_count
  , double
  , do_not_replace
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('typeid') }} AS typeid
  , updated
  , userid
  , default_ship_period
  , premium_meals_valid
  , has_blacksheet_tray
  , trusted_payment_source
FROM {{ source('combined_api_v3', 'subscriptionactivity') }}

{{ load_incrementally() }}
