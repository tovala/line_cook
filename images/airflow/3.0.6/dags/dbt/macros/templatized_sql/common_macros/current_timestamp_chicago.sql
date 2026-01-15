-- Returns current time in Chicago
{%- macro current_timestamp_chicago() -%}
convert_timezone('America/Chicago', CURRENT_TIMESTAMP)
{%- endmacro -%}