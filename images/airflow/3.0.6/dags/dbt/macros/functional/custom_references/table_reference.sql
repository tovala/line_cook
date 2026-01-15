-- Returns a reference to the specified table if running in prod (thus creating a dependency) or the prod version of a table o/w
-- If table_reference refers to a dry table, base schema does not need to be specified (but the table alias must be dry_{table_name})
{% macro table_reference(table_name, base_schema='dry', db='MASALA') %}
  {%- set target_relation = api.Relation.create(identifier=table_name, schema=base_schema, database=db, type='table') -%}

  {%- if base_schema == 'dry' -%}
    {%- set table_alias = base_schema + '_' + table_name -%}
  {%- else -%}
    {%- set table_alias = table_name -%}
  {%- endif -%}

  {%- if target.name == 'prod' -%}
    {{ ref(table_alias) }}
  {%- else -%}
    {{ return(target_relation) }}
  {%- endif -%}
{% endmacro %}