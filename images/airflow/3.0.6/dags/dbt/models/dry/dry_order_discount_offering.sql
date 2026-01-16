{{ dry_config('order_discount_offering',
              tags = ['marketing_incentives']
            ) 
}}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('description') }} AS description
  , {{ clean_string('incentive_id') }} AS incentive_id
  , {{ clean_string('attribution_type') }} AS attribution_type
  , last_available_term
  , max_uses 
  , minimum_order_cents
  , minimum_order_meal_count 
  , {{ clean_string('marketing_text_override') }} AS marketing_text_override
  , created
  , updated
FROM {{ source('combined_api_v3', 'order_discount_offering') }}

{{ load_incrementally() }}
