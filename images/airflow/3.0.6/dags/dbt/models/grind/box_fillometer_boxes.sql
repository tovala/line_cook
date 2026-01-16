SELECT
    {{ clean_string('raw_data:order:orderID::STRING') }} AS order_id
    , bx.value:dbLinkInfo:OrderBoxID AS box_db_id
    , TRY_TO_NUMBER(box_sizes.key)::INTEGER AS box_vertical_size
    , TRY_TO_NUMBER(bx.value:boxSize:HorizontalSize::STRING)::INTEGER AS horizontal_size
    , TRY_TO_NUMBER(bx.value:boxSize:VerticalSize::STRING)::INTEGER AS vertical_size
    , bx.value:boxSize:Name AS box_name
    , TRY_TO_NUMBER(bx.value:boxSize:OverallDimensions:PhysicalHeight::STRING)::INTEGER AS physical_height
    , TRY_TO_NUMBER(bx.value:boxSize:OverallDimensions:PhysicalLength::STRING)::INTEGER AS physical_length
    , TRY_TO_NUMBER(bx.value:boxSize:OverallDimensions:PhysicalWeight::STRING)::INTEGER AS physical_weight
    , TRY_TO_NUMBER(bx.value:boxSize:OverallDimensions:PhysicalWidth::STRING)::INTEGER AS physical_width
    , UUID_STRING() AS box_id
    , bx.value:itemsBySlot AS items_by_slot
FROM {{source('chili', 'box_fillometer_logs')}},
LATERAL FLATTEN(input => raw_data:boxes) box_sizes,
LATERAL FLATTEN(input => box_sizes.value) bx