
SELECT 
    meal_sku_id::INTEGER AS meal_sku_id
    , CASE
        WHEN actual_ingredient_cost_chicago = '-' THEN NULL -- handle edge cases
        ELSE replace(actual_ingredient_cost_chicago, '$', '') -- remove $ signs
    END::FLOAT AS historical_ingredient_cost_chicago
FROM {{ source('meal_tags', 'meal_sku_costs') }}
