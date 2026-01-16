{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://tovala-data-engineering/sftp_ccc_output/',
    stage_file_format_string = "(type = 'JSON' compression = GZIP)",
    stage_name = 'direct_mail',
    copy_into_options = "PATTERN='(.*).gz'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , {{ filename_timestamp_extractor() }} AS upload_time
FROM {{ external_stage() }}
