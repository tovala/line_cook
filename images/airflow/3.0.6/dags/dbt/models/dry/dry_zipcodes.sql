{{ dry_config('zipcodes', primary_key='zip') }}

SELECT
  estimated_delivery_days
  , {{ clean_string('shipping_company') }} AS shipping_company
  , {{ clean_string('shipping_service') }} AS shipping_service
  , {{ clean_string('ship_day_of_the_week') }} AS ship_day_of_the_week
  , {{ clean_string('ship_origin') }} AS ship_origin
  , {{ clean_string('state') }} AS state
  , transit_days
  , updated
  , {{ clean_string('zip') }} AS zip
FROM {{ source('combined_api_v3', 'zipcodes') }}

{{ load_incrementally() }}
