{{ dry_config('subterm_zipcodes_shipping_information') }}

SELECT
  created 
  , estimated_delivery_days
  , {{ clean_string('id') }} AS id
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('ship_day_of_the_week') }} AS ship_day_of_the_week
  , {{ clean_string('shipping_company') }} AS shipping_company
  , {{ clean_string('shipping_origin') }} AS shipping_origin
  , {{ clean_string('shipping_service') }} AS shipping_service
  , {{ clean_string('subterm_zipcode_id') }} AS subterm_zipcode_id
  , transit_days
  , updated
FROM {{ source('combined_api_v3', 'subterm_zipcodes_shipping_information') }}

{{ load_incrementally() }}
