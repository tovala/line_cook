SELECT
    {{ clean_string('raw_data:order:orderID') }} AS order_id 
    , TRY_TO_NUMBER(raw_data:order:termID::STRING)::INTEGER AS term_id
    , {{ clean_string('raw_data:order:facilityNetwork') }} AS facility_network
    , TRY_TO_NUMBER(raw_data:order:cycle::STRING)::INTEGER AS cycle
    , TRY_TO_NUMBER(raw_data:order:userInfo:mealCount::STRING)::INTEGER AS meal_count
FROM {{source('chili', 'box_fillometer_logs')}}