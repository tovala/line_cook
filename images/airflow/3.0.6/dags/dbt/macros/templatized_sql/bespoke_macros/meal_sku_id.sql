{%- macro meal_sku_id(barcode_field) -%}
TRY_TO_NUMERIC(NULLIF(TRIM(SPLIT_PART({{ barcode_field }}, '|', 2)), ''))::INTEGER
{%- endmacro -%}