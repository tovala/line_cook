{% macro table_columns() -%}
"year" VARCHAR
, "month" VARCHAR
, d2c FLOAT
, amazon FLOAT
, costco FLOAT
, loaded_at TIMESTAMP
{%- endmacro %}