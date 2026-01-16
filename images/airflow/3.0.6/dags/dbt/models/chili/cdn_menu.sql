{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://menu-delivery-files-release-20230727190758320400000002/menu/',
    stage_file_format_string = "(type = 'JSON')",
    stage_name = 'cdn_menu',
    copy_into_options = "PATTERN='(.*).json'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
--SW Team naming convention doesn't have a timestamp
FROM {{ external_stage() }}