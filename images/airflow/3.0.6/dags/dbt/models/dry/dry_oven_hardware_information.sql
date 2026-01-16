{{ dry_config('oven_hardware_information') }}

SELECT 
  {{ clean_string('channel') }} AS channel
  , created
  , date_shipped
  , {{ clean_string('generation_marker') }} AS generation_marker
  , {{ clean_string('id') }} AS id
  , {{ clean_string('model') }} AS model
  , {{ clean_string('serial_number') }} AS serial_number
  , {{ clean_string('shipment_sku') }} AS shipment_sku
  , {{ clean_string('source_key') }} AS source_key
  , {{ clean_string('sub_channel') }} AS sub_channel
  , updated
FROM {{ source('combined_api_v3', 'oven_hardware_information') }}
{{ load_incrementally() }}
