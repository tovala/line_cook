CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }} (
    {% import params.table_columns_file as columns %}
    {{ columns.table_columns() }}
);