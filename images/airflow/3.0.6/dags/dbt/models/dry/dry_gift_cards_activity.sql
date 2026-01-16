{{ dry_config('gift_cards_activity') }}

SELECT
  {{ clean_string('action') }} AS action
  , amount_cents
  , created
  , {{ clean_string('gift_card_id') }} AS gift_card_id
  , {{ clean_string('id') }} AS id
  , {{ clean_string('payment_id') }} AS payment_id
  , {{ clean_string('products_purchase_id') }} AS products_purchase_id
  , {{ clean_string('stripe_orderid') }} AS stripe_orderid
  , updated
  , {{ clean_string('type') }} AS type
  , {{ clean_string('notes') }} AS notes
  , {{ clean_string('cs_zd_ticket') }} AS cs_zd_ticket 
  , {{ clean_string('cs_agent') }} AS cs_agent 
  , {{ clean_string('cs_reason') }} AS cs_reason  
FROM {{ source('combined_api_v3', 'gift_cards_activity') }}

{{ load_incrementally() }}
