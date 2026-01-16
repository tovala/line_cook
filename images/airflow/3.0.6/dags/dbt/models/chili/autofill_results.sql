{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://tovala-autofill/newAutofill/',
    stage_file_format_string = "(type = 'JSON')",
    stage_name = 'newAF',
    copy_into_options = "PATTERN='(.*).json'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , LOWER(METADATA$FILENAME) AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP(REGEXP_SUBSTR(filename, '-(\\d{10}).json', 1, 1, 'e'))) AS run_time
  , TRY_TO_NUMERIC(REGEXP_SUBSTR(filename, 'term-(\\d{3,4})', 1, 1, 'e'))::INTEGER AS term_id
  , TRY_TO_NUMERIC(REGEXP_SUBSTR(filename, 'cycle-(1|2)', 1, 1, 'e'))::INTEGER AS cycle
  , REGEXP_SUBSTR(filename, 'facility-(slc|chicago)', 1, 1, 'e') AS facility_network
  , TRY_TO_NUMERIC(REGEXP_SUBSTR(filename, 'mpr-(\\d+)', 1, 1, 'e'))::INTEGER AS meals_per_round
FROM {{ external_stage() }}