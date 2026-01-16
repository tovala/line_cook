
SELECT
    userid AS customer_id
    , termid AS term_id 
    , selected_automatically AS is_autoselection
    , MIN(created) AS first_meal_selection_time
FROM {{ source('combined_api_v3', 'mealselections') }}
GROUP BY 1,2,3
