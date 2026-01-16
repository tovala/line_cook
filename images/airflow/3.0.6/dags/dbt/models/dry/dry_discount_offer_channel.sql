{{ dry_config('discount_offer_channel', 
              tags = ['marketing_incentives']
            ) 
}}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('description') }} AS description
  , {{ clean_string('offer_id') }} AS offer_id
  , {{ clean_string('segment_type') }} AS segment_type
  , created
  , updated
FROM {{ source('combined_api_v3', 'discount_offer_channel') }}

{{ load_incrementally() }}
