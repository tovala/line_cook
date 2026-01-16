
SELECT 
    ms.id AS meal_selection_id
    , ms.userid AS customer_id 
    , ms.termid AS term_id
    , mo.meal_order_id
    , mo.is_fulfilled 
    , COALESCE(ms.selected_automatically, FALSE) AS is_autoselection 
    , m.meal_sku_id
    , m.production_cd 
    , mmo.menu_meal_id
    , m.title AS meal_title 
    , m.subtitle AS meal_subtitle
    , ms.created AS meal_selection_time
FROM {{ table_reference('mealselections') }} ms
INNER JOIN {{ ref('meal_skus') }} m 
    ON ms.mealid = m.meal_sku_id
LEFT JOIN {{ ref('meal_orders') }} mo 
    ON ms.userid = mo.customer_id
    AND ms.termid = mo.term_id 
LEFT JOIN {{ ref('menu_meal_offerings') }} mmo
    ON mo.subterm_id = mmo.subterm_id
    AND ms.mealid = mmo.meal_sku_id
