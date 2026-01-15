-- Parses a timestamp in the format YYYY-MM-DD HH:MM:SS from a filename
{%- macro filename_timestamp_extractor() -%}
  TRY_TO_TIMESTAMP(REGEXP_SUBSTR(filename, '\\d{4}-\\d{2}-\\d{2}') || ' ' || REGEXP_SUBSTR(filename, '\\d{2}:\\d{2}:\\d{2}'))
{%- endmacro -%}