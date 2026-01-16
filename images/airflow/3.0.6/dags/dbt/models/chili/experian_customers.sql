{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://tovala-data-engineering/experian/',
    stage_file_format_string = "(type = 'JSON' compression = GZIP)",
    stage_name = 'experian',
    copy_into_options = "PATTERN='(.*).gz'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , {{ filename_timestamp_extractor() }} AS upload_time
  , raw_data:lead_id::INTEGER AS customer_id
  , REGEXP_SUBSTR(filename, 'experian-parsed-([0-9]+)-', 1, 1, 'e') AS transaction_id
FROM {{ external_stage() }}