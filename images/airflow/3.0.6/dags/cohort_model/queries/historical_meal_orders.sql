{% macro table_columns() -%}
  term_id INTEGER
  , cohort INTEGER 
  , order_count INTEGER 
  , meal_count INTEGER
{%- endmacro %}