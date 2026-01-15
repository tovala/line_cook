-- Retrieves all of the dry tables derived from the specified source_schema
{%- macro fetch_sourced_tables(source_schema) -%}
  -- Graph variable isn't available during parsing, this prevents it from throwing up an error
  {% if graph == {} %}
    {{ return([]) }}
  {% endif %}
  {%- set test_tables = [] -%}
  {% for node in graph.nodes.values()
     | selectattr("resource_type", "equalto", "model") 
     | selectattr("config.schema", "equalto", "dry")
     | selectattr("sources", "ne", []) %}
    {%- if node.sources[0][0] == source_schema and node.config.unique_key == 'id' -%}
      -- This is the case for combined_api only
      {%- if node.sources[0][0] == 'combined_api_v3' -%}
        {%- set base_table = node.config.alias -%}
      {%- else -%}
        {%- set base_table = node.config.meta.base_table -%}
      {%- endif -%}
      {%- set test_tables=test_tables.append([base_table, node.config.alias]) -%}    
    {%- endif -%}
  {% endfor %}
  {{ return(test_tables) }}
{%- endmacro -%}

-- Checks that the number of rows in the dry table is equal to the number of rows in the landing table minus any rows in delete_logs
{% macro delete_test(source_table, dry_table_alias, source_schema) %}
  ((SELECT COUNT(*) 
    FROM {{ source_schema }}.{{ source_table }} ms 
    LEFT JOIN {{ source_schema }}.DELETE_LOGS dl 
    ON ms.id::STRING = dl.row_id AND dl.table_name = '{{ source_table }}'
    WHERE dl.row_id IS NULL
    )
  EXCEPT 
  (SELECT COUNT(*)
   FROM {{ get_dry_schema() }}.{{ dry_table_alias }}))
{% endmacro %}

-- Runs all delete tests for a source schema, if run as is_local=True it outputs any failing tables
{% macro run_all_delete_tests(source_schema, is_local=False, excluded_tables=[]) %}
  {%- set table_names = fetch_sourced_tables(source_schema)-%}

  {% set delete_test_statements = [] %}

  {% for t_name in table_names %}
    {%- if t_name[1] not in excluded_tables -%}
      {%- set test = delete_test(t_name[0], t_name[1], source_schema) -%}
      {%- set delete_test_statements = delete_test_statements.append(test) -%}
    {%- endif -%}
  {% endfor %}
  
  {%- if not is_local -%}
    {{ delete_test_statements|join(' UNION ALL ') }}
  {%- else -%}
    {%- for delete_test in delete_test_statements -%}
      {%- call statement('run_delete_test', fetch_result=True) -%}
        {{ delete_test }}
      {%- endcall -%}
      {%- if load_result('run_delete_test')['data']|length > 0 -%}
        {{ log(delete_test, True) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
