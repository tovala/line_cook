-- DATA SOURCE: https://us-west-2.console.aws.amazon.com/lambda/home?region=us-west-2#/functions/ShipmentFileConverter?tab=configuration
{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://tovala-data-engineering/shipment_file/',
    stage_file_format_string = "(type = 'JSON' compression = GZIP)",
    stage_name = 'shipment_file',
    copy_into_options = "PATTERN='(.*)master.gz'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
  , {{ filename_timestamp_extractor() }} AS upload_time
  , REGEXP_REPLACE(REGEXP_SUBSTR(FILENAME, 'term\\d{3,4}'), 'term', '')::INTEGER AS term_id
  , REGEXP_REPLACE(REGEXP_SUBSTR(FILENAME, 'cycle[1|2]'), 'cycle', '')::INTEGER AS cycle_id
  , REGEXP_SUBSTR(filename, 'chicago|slc') AS facility_network
FROM {{ external_stage() }}