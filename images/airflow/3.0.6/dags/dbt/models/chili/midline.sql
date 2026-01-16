{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://oven-manufacturing-logs/',
    stage_file_format_string = "(type = 'JSON')",
    stage_name = 'midline',
    copy_into_options = "PATTERN='(midline|(midline[1-9]))/.*\\.json'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , {{ filename_unix_timestamp_extractor("REGEXP_SUBSTR(METADATA$FILENAME, '([0-9]+)\\.json$', 1, 1, 'e')")}} AS test_run_time 
FROM {{ external_stage() }}
