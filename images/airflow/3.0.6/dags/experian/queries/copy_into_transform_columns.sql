TRY_PARSE_JSON($1) AS raw_data
, METADATA$FILENAME AS filename
, TO_VARIANT(CURRENT_TIMESTAMP()::TIMESTAMPTZ) AS updated 
, raw_data:lead_id AS customer_id