{%- macro filename_unix_timestamp_extractor(pattern) -%}
  CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_NTZ(CAST({{pattern}} AS BIGINT)))
{%- endmacro -%}
