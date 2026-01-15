{%- macro verify_schemas() -%}
  {%- if target.name == 'prod' and graph.nodes is defined -%}
    -- Fetch all the schemas created by dbt 
    {%- set all_schemas=[] -%}
    {%- for node in graph.nodes.values()
      | selectattr("resource_type", "in", ["model", "seed", "snapshot"])-%}
      {%- if node.schema not in all_schemas -%}
        {%- set all_schemas=all_schemas.append(node.schema) -%}
      {%- endif -%}
    {%- endfor -%}
    -- Ensure they exist
    {%- for schema in all_schemas -%}
      {%- call statement('get_schemata', fetch_result=True) %}
        CREATE SCHEMA IF NOT EXISTS {{schema}};
      {%- endcall -%}
    {%- endfor -%}
  {%- endif -%}
{%- endmacro -%}