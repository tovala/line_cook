{{ dry_config('orders_pos_mappings', primary_key='stitch_id') }}

SELECT
  created 
  , {{ clean_string('orders_pos_id') }} AS orders_pos_id
  , {{ clean_string('orders_pos_invoice_id') }} AS orders_pos_invoice_id
  , {{ clean_string('source_order_id') }} AS source_order_id
  , {{ clean_string('source_order_type') }} AS source_order_type
  , {{ clean_string('stitch_id') }} AS stitch_id
  , updated
FROM {{ source('combined_api_v3', 'orders_pos_mappings') }}

{{ load_incrementally() }}
