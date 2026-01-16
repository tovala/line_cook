{{ dry_config('location', primary_key = 'userid') }}

SELECT
  {{ clean_string('address_line1') }} AS address_line1
  , {{ clean_string('address_line2') }} AS address_line2
  , {{ clean_string('address_type') }} AS address_type
  , {{ clean_string('city') }} AS city
  , {{ clean_string('country') }} AS country
  , estimated_delivery_days
  , {{ clean_string('name') }} AS name
  , {{ clean_string('phone') }} AS phone
  , {{ clean_string('postal_code') }} AS postal_code
  , {{ clean_string('ship_day_of_the_week') }} AS ship_day_of_the_week
  , {{ clean_string('state') }} AS state
  , updated
  , userid
FROM {{ source('combined_api_v3', 'location') }}

{{ load_incrementally() }}
