{{ dry_config('products') }}

SELECT
  activate_subscription
  , active
  , charge_for_subscription
  , {{ clean_string('charge_sku') }} AS charge_sku
  , commitment_complainer_weeks
  , commitment_meals
  , commitment_weeks
  , commitment_duration_interval
  , create_fulfillment
  , {{ clean_string('description') }} AS description
  , gift_cents
  , {{ clean_string('id') }} AS id
  , meals_amount
  , {{ clean_string('membership_type_id') }} AS membership_type_id
  , {{ clean_string('parent') }} AS parent
  , price_cents
  , shipping_cost_added
  , {{ clean_string('shipment_sku') }} AS shipment_sku
  , {{ clean_string('type') }} AS type
  , updated
  , {{ clean_string('warranty_period') }} AS warranty_period
  , warranty_unit
FROM {{ source('combined_api_v3', 'products') }}

{{ load_incrementally() }}
