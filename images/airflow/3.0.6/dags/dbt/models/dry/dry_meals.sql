{{ dry_config('meals') }}

SELECT
  {{ clean_string('chef_image') }} AS chef_image
  , {{ clean_string('created_by') }} AS created_by
  , {{ clean_string('customer_quote') }} AS customer_quote
  , {{ clean_string('customer_quote_by') }} AS customer_quote_by
  , expiration
  , id
  , {{ clean_string('ingredients') }} AS ingredients
  , {{ clean_string('meal_fact') }} AS meal_fact
  , {{ clean_string('meal_prep_steps') }} AS meal_prep_steps
  , {{ clean_string('nf_meal_code') }} AS nf_meal_code
  , TRY_PARSE_JSON(nutritionalinfo) AS nutritionalinfo
  , prep_in_seconds
  , price
  , {{ clean_string('short_subtitle') }} AS short_subtitle
  , sold_out_count
  , {{ clean_string('story') }} AS story
  , {{ clean_string('subtitle') }} AS subtitle
  , termid
  , {{ clean_string('title') }} AS title
  , updated
  , {{ clean_string('expiration_offset') }} AS expiration_offset
  , first_ship_period_availability
  , second_ship_period_availability
  , baseprice_cents
  , surcharge_cents
FROM {{ source('combined_api_v3', 'meals') }}

{{ load_incrementally() }}
