{{ dry_config('zipcode_shipping_information') }}

SELECT
  available
  , created
  , default_subterm
  , estimated_delivery_days
  , {{ clean_string('id') }} AS id
  , {{ clean_string('marketing_lever_color') }} AS marketing_lever_color
  , {{ clean_string('marketing_lever_string') }} AS marketing_lever_string
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('ship_day_of_the_week') }} AS ship_day_of_the_week
  , ship_period
  , updated
  , {{ clean_string('zipcode') }} AS zipcode
  , {{ clean_string('shipping_service') }} AS shipping_service
  , transit_days
  , {{ clean_string('shipping_company') }} AS shipping_company
  , {{ clean_string('shipping_origin') }} AS shipping_origin
  , {{ clean_string('facility_network') }} AS facility_network
FROM {{ source('combined_api_v3', 'zipcode_shipping_information') }}

{{ load_incrementally() }}
