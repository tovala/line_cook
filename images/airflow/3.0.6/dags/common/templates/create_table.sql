CREATE OR REPLACE TABLE TEST_TAYLOR_BRINE.{{ params.table_name }} (
    {% import params.table_columns_file as columns %}
    {{ columns.table_columns() }}
);