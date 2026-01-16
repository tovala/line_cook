CREATE OR REPLACE TABLE {{ params.parent_database }}.{{ params.schema_name }}.{{ params.table_name }} (
    {% import params.table_columns_file as columns %}
    {{ columns.table_columns() }}
);