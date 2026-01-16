{{ dry_config('orderboxes') }}

SELECT
  box_type
  , created
  , {{ clean_string('id') }} AS id
  , meals_count
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('orderfulfillment_id') }} AS orderfulfillment_id
  , {{ clean_string('shipment_status') }} AS shipment_status
  , termid
  , {{ clean_string('trackingnumber') }} AS trackingnumber
  , updated
  , userid
FROM {{ source('combined_api_v3', 'orderboxes') }}

{{ load_incrementally() }}
