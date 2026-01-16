{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://cdn.tovala.com/presets/',
    stage_file_format_string = "(type = 'JSON')",
    stage_name = 'tovala_preset',
    copy_into_options = "PATTERN='(.*)presets.json(.*)'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
FROM {{ external_stage() }}