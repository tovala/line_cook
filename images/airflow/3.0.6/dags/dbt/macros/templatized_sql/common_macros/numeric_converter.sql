-- Removes '$', ',', or '%' then converts to a numeric with 2 places after the decimal
{%- macro numeric_converter(d_string) -%}
  TO_NUMBER(REGEXP_REPLACE({{ d_string }}, '\\$|,|%', ''), 10, 2)
{%- endmacro -%}