
SELECT
    order_id
    , box_id
    , UUID_STRING() AS slotitem_id
    , box_vertical_size
    , box_db_id
    , slots.key AS slot_number
    , TRY_TO_NUMBER(slotitems.value:mealID::STRING)::INTEGER AS meal_id
    , TRY_TO_NUMBER(slotitems.value:mealCode::STRING)::INTEGER AS meal_code
    , {{ clean_string('slotitems.value:name') }} AS meal_name
    , {{ clean_string('slotitems.value:listingID') }} AS listing_id
    , TRY_TO_NUMBER(slotitems.value:verticalSize::STRING)::INTEGER AS vertical_size
    , TRY_TO_NUMBER(slotitems.value:fitGradient::STRING)::INTEGER AS fit_gradient
    , ARRAY_TO_STRING(slotitems.value:proteins, ',') AS proteins
FROM {{ref('box_fillometer_boxes')}},
LATERAL FLATTEN(input => items_by_slot) slots,
LATERAL FLATTEN(input => slots.value) slotitems
