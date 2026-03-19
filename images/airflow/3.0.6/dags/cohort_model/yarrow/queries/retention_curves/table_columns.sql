{% macro table_columns() -%}
id VARCHAR PRIMARY KEY
, curve_definition_id VARCHAR
, created TIMESTAMP_TZ
{% for week_num in range(79) %}
, week_{{ week_num }} FLOAT
{% endfor %}
CONSTRAINT fk_curve_id FOREIGN KEY (curve_id) REFERENCES retention_curve_definitions(id)
{%- endmacro %}