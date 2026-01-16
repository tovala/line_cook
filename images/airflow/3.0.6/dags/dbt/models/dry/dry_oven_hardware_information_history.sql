{{ dry_config('oven_hardware_information_history') }}

SELECT 
  {{ clean_string('action') }} AS action
  , {{ clean_string('channel') }} AS channel
  , created
  , date_shipped
  , {{ clean_string('id') }} AS id
  , {{ clean_string('model') }} AS model
  , {{ clean_string('oven_hardware_information_id') }} AS oven_hardware_information_id
  , {{ clean_string('serial_number') }} AS serial_number
  , {{ clean_string('shipment_sku') }} AS shipment_sku
  , {{ clean_string('sub_channel') }} AS sub_channel
FROM {{ source('combined_api_v3', 'oven_hardware_information_history') }}
{{ load_incrementally(bookmark='created') }}
