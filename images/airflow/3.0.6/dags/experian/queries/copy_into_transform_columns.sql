TRY_PARSE_JSON($1) AS raw_data
, METADATA$FILENAME AS filename
, CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
, raw_data:lead_id::INTEGER AS customer_id