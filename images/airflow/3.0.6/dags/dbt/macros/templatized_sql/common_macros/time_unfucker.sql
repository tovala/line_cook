-- Takes time in the format H:MMAM or HH:MMPM
{%- macro time_unfucker(time_string) -%}
  CASE WHEN {{ time_string }} LIKE '%AM' 
       THEN LPAD(REGEXP_SUBSTR({{ time_string }}, '^\\d{1,2}'), 2, '0') || REGEXP_SUBSTR({{ time_string }}, ':\\d{2}')
       ELSE LPAD((REGEXP_SUBSTR({{ time_string }}, '^\\d{1,2}')%12)+12, 2, '0') || REGEXP_SUBSTR({{ time_string }}, ':\\d{2}')
  END
{%- endmacro -%}