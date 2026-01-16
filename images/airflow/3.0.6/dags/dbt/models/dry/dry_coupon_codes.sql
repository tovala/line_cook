{{ dry_config('coupon_codes', 
              primary_key = 'code') }}

SELECT
  {{ clean_string('code') }} AS code
  , created
  , credit_amount
  , {{ clean_string('description') }} AS description
  , dollar_amount
  , dollar_percent
  -- Offer channel that is associated with a coupon code 
  -- We find an active offer linked to this channel and then will find the coupon code associated with the channel
  , linked_offer_channel 
  , max_food_orders
  , max_redemptions
  , meals_amount
  , {{ clean_string('notes') }} AS notes
  , promo_duration
  , promo_expiration
  , {{ clean_string('promo_interval') }} AS promo_interval
  , {{ clean_string('promo_type') }} AS promo_type
  , redeem_by
  , {{ clean_string('sku') }} AS sku
  , {{ clean_string('stripe_code') }} AS stripe_code
  , updated
  , userid
FROM {{ source('combined_api_v3', 'coupon_codes') }}

{{ load_incrementally() }}
