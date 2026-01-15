{%- macro date_unfucker(date_string) -%}
  TO_DATE(LPAD(REGEXP_SUBSTR({{ date_string }}, '^\\d{1,2}'), 2, '0') || '/' 
          || LPAD(REGEXP_REPLACE(REGEXP_SUBSTR({{ date_string }}, '/\\d{1,2}/'), '/', ''), 2, '0') || '/'
          || LPAD(REGEXP_SUBSTR({{ date_string }}, '\\d{2,4}$'), 4, '20'), 'MM/DD/YYYY')
{%- endmacro -%}