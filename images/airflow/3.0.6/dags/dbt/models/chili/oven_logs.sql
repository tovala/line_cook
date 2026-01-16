{{
  config(
    materialized='from_external_stage',
    schema='kinesis',
    stage_url = 's3://tovala-kinesis-data/',
    stage_file_format_string = "(type = 'JSON' compression = GZIP)",
    stage_name = 'oven_logs',
    copy_into_options = "PATTERN='(.*).gz'"
  )
}}

SELECT 
    CASE WHEN TRY_PARSE_JSON($1):payload IS NOT null
         THEN OBJECT_DELETE(
                TRY_PARSE_JSON(
                  CONCAT(
                    SUBSTR(TO_JSON($1), 1, LEN(TO_JSON($1))-1),
                    ',',
                    SUBSTR(TRY_PARSE_JSON(TRY_PARSE_JSON($1):payload)::STRING, 2)
                  )
                ),
                'payload'
              )
         ELSE TRY_PARSE_JSON($1)
    END AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , REPLACE(REGEXP_SUBSTR(FILENAME, '^\\d{4}(/)\\d{2}(/)\\d{2}'), '/', '-')::DATE AS original_file_date
FROM {{ external_stage() }}