{{ dry_config('user_term_order_offers', 
              tags = ['marketing_incentives']
            ) 
}}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('offer_id') }} AS offer_id
  , {{ clean_string('channel_id') }} AS channel_id
  , {{ clean_string('uto_id') }} AS uto_id
  , applied AS is_applied 
  , created
  , updated
FROM {{ source('combined_api_v3', 'user_term_order_offers') }}

{{ load_incrementally() }}
