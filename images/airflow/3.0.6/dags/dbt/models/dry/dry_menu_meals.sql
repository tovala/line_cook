{{ dry_config('menu_meals') }}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('menu_id') }} AS menu_id
  , meal_id
  , {{ clean_string('production_code') }} AS production_code
  , main_display_order
  , expiration_date
  , {{ clean_string('notes') }} AS notes
  , created
  , updated
FROM {{ source('combined_api_v3', 'menu_meals') }}

{{ load_incrementally() }}