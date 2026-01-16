{{ dry_config('gift_cards') }}

SELECT
  {{ clean_string('buyer_email') }} AS buyer_email
  , {{ clean_string('buyer_message') }} AS buyer_message
  , {{ clean_string('buyer_name') }} AS buyer_name
  , created
  , {{ clean_string('id') }} AS id
  , redeemed_by
  , {{ clean_string('redeem_code') }} AS redeem_code
  , updated
  , userid
FROM {{ source('combined_api_v3', 'gift_cards') }}

{{ load_incrementally() }}
