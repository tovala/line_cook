-- Grants select access to views in the sigma_input_table schema to the ETL

{%- macro view_read_prehook() -%}
  {{ alter_access_on('SELECT', ['ALL VIEWS IN SCHEMA'], ['SIGMA_INPUT_TABLES'], 'ROLE', 'ETL') }}
{%- endmacro -%}