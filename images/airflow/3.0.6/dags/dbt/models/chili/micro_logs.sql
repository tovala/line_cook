{{
  config(
    materialized='from_external_stage',
    schema='kinesis',
    stage_url = 's3://tovala-micro-logs/',
    stage_file_format_string = "(type = 'JSON' compression = GZIP)",
    stage_name = 'micro_logs',
    copy_into_options = "PATTERN='(.*).gz'"
  )
}}

SELECT
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , CASE WHEN SPLIT_PART(filename, '/', 2) IN ('micro-2019', 'micro-2020', 'micro-2021')
         THEN (LEFT(SPLIT_PART(filename, '/', 3), 4) || '-' || SUBSTRING(SPLIT_PART(filename, '/', 3), 5, 2) || '-' || RIGHT(SPLIT_PART(filename, '/', 3), 2))::DATE
         ELSE REPLACE(REGEXP_SUBSTR(FILENAME, '^\\d{4}(/)\\d{2}(/)\\d{2}'), '/', '-')::DATE
    END AS original_file_date
FROM {{ external_stage() }}