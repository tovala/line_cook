
SELECT 
    REPLACE(raw_data:userid::STRING, ',', '')::INTEGER AS customer_id
    , raw_data:added_to_slack::BOOLEAN AS is_slackable
    , raw_data:signed_nda::BOOLEAN AS has_signed_nda
    , try_to_date({{clean_string('raw_data:birthday::STRING')}}) AS birthday
    , try_to_date({{clean_string('raw_data:cab_start_date::STRING')}}) AS membership_start_date
    , try_to_date({{clean_string('raw_data:cab_left_date::STRING')}}) AS membership_end_date
FROM {{ source('chili','customer_advisory_board') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY upload_time DESC) = 1
