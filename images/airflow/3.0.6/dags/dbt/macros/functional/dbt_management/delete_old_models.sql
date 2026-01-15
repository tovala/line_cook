{%- macro cleanup_old_models() -%}
  {%- set all_schemas=[] -%}
  {%- for node in graph.nodes.values()
    | selectattr("resource_type", "in", ["model", "seed", "snapshot"])-%}
    {%- if node.schema not in all_schemas -%}
      {%- set all_schemas=all_schemas.append(node.schema) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for s in all_schemas -%}
    {{ cleanup_old_schema(s) }}
  {%- endfor -%}
{%- endmacro -%}

{%- macro cleanup_old_schema(schema_test)-%}
  {{ log('Cleaning up models in {}'.format(schema_test),True) }}
  {%- set all_tables=[] -%}
  {%- for node in graph.nodes.values()
    | selectattr("resource_type", "in", ["model", "seed", "snapshot"])
    | selectattr("schema", "equalto", schema_test)-%}
    {%- set all_tables=all_tables.append("'" ~ node.alias ~ "'") -%}
  {%- endfor -%}
  {%- set table_test=all_tables|join(', ')-%}

  {%- call statement('test_against_db', fetch_result=True) %}
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE LOWER(TABLE_SCHEMA) = '{{ schema_test }}'
      AND NOT LOWER(TABLE_NAME) IN ({{ table_test }});
  {%- endcall -%}

  {%- set table_list = load_result('test_against_db') -%}
  {%- set tables = table_list['data'] | map(attribute=0) | list %}

  {%- for t in tables -%}
    {{ clean_up_table(schema_test, t) }}
  {%- endfor -%}
  
{%- endmacro -%}