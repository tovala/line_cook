-- Grants ownership of tables created by dbt to ETL role - allows for cleanup by Compost
-- When running outside of prod, this grants usage to the TECHNICAL role 

{%- macro grant_dbt_access(schemas) -%}
  {{ alter_access_on('OWNERSHIP', ['SCHEMA', 'ALL TABLES IN SCHEMA'], schemas, 'ROLE', 'ETL', optional_qualifier = 'COPY CURRENT GRANTS') }}
  {%- if target.name == 'test' -%}
    {{ alter_access_on('USAGE', ['SCHEMA'], schemas, 'ROLE', 'TECHNICAL') }}
    {{ alter_access_on('SELECT', ['ALL TABLES IN SCHEMA'], schemas, 'ROLE', 'TECHNICAL') }}
  {%- endif -%}
{%- endmacro -%}