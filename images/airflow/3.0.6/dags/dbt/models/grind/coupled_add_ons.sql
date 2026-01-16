SELECT 
  TRY_TO_NUMERIC(add_on_meal_id::VARCHAR)::INTEGER AS add_on_meal_id
  , add_on_title
  , term_id
  , meal_price
  , offered_with_meal_id
FROM {{ source('sigma_input_tables', 'menu_add_ons_input') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY add_on_meal_id, offered_with_meal_id ORDER BY last_updated_at DESC) = 1
