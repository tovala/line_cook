-- Returns current time in UTC
{%- macro current_timestamp_utc() -%}
convert_timezone('UTC', CURRENT_TIMESTAMP)
{%- endmacro -%}