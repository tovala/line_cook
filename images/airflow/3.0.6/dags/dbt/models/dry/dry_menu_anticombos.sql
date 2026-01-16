{{ dry_config('menu_anticombos') }}

SELECT
  {{ clean_string('id') }} AS id
  , {{ clean_string('menu_id') }} AS menu_id
  , {{ clean_string('menu_meal_id') }} AS menu_meal_id
  , {{ clean_string('anti_combo_menu_meal_id') }} AS anti_combo_menu_meal_id
  , is_sideswap
  , updated
FROM {{ source('combined_api_v3', 'menu_anticombos') }}

{{ load_incrementally() }}
