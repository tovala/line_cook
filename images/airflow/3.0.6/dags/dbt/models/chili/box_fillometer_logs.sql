{{
  config(
    materialized='from_external_stage',
    schema='chili',
    stage_url = 's3://misevala/prod/box_fillometer/order_boxes/',
    stage_file_format_string = "(type = 'JSON' strip_outer_array = true)",
    stage_name = 'box_fillometer',
    copy_into_options = "PATTERN='prod/box_fillometer/order_boxes/([0-9]+)-([A-Za-z0-9]+)-cycle([0-9]+).orders.json'"
  )
}}

SELECT 
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated 
FROM {{ external_stage() }}