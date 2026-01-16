{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://retool-data-uploads/customer-advisory-board/',
    stage_file_format_string = "(type = 'JSON')",
    stage_name = 'CAB',
    copy_into_options = "FILE_FORMAT = (type = 'JSON')"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , {{ filename_timestamp_extractor() }} AS upload_time
FROM {{ external_stage() }}
