{{ dry_config('reactivation_credits') }}

SELECT
  id
  , gift_card_id 
  , retail_channel
  , intended_conversion_event
  , proximal_ordering_term
  , created 
FROM {{ source('combined_api_v3', 'reactivation_credits') }}

{{ load_incrementally(bookmark='created') }}
