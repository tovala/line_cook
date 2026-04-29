{% macro table_columns() -%}
oven_sales_date DATE
, estimated_d2c_sales NUMBER
, estimated_amazon_sales NUMBER
, estimated_other_sales NUMBER
, loaded_at TIMESTAMP
{%- endmacro %}