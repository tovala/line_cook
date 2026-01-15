{% macro clean_up_table(schema_name, table_name) %}
  {{ log('Cleaning up table {}.{}...'.format(schema_name, table_name), True) }}

  {% set query %}
    DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }};
    COMMIT;
  {% endset %}
  {% do run_query(query) %}

{% endmacro %}

{% macro clean_up_schema(schema_name) %}
  {{ log('Cleaning up schema {}...'.format(schema_name), True) }}

  {%- call statement('get_tables', fetch_result=True) %}
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{ schema_name }}';
  {%- endcall -%}

  {%- set table_list = load_result('get_tables') -%}
  {%- set tables = table_list['data'] | map(attribute=0) | list %}

  {% for table in tables %}
    {{ clean_up_table(schema_name, table) }}
  {% endfor %}

  {% set query %}
    DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE;
  {% endset %}
  {% do run_query(query) %}

{% endmacro %}

{% macro clean_up_branch() %}
  {% if target.name == 'prod' %}
    {{ log('Do not do that.', True) }}
  
  {% else %}
    {{ log('Cleaning up branch {} ...'.format(target.schema), True) }}

    {%- call statement('get_schemata', fetch_result=True) %}
      SELECT SCHEMA_NAME 
      FROM INFORMATION_SCHEMA.SCHEMATA
      WHERE SCHEMA_NAME ILIKE '{{ target.schema }}%';
    {%- endcall -%}

    {%- set schema_list = load_result('get_schemata') -%}
    {%- set schemas = schema_list['data'] | map(attribute=0) | list %}
    
    {% for schema in schemas %}
      {{ clean_up_schema(schema) }}
    {% endfor %}

    {{ log('Deleting from delete_bookmarks {} ...'.format(target.schema), True) }}
    {% set query %}
      DELETE FROM {{ var('delete_bookmarks_table') }}
      WHERE SCHEMA_NAME ILIKE '{{ target.schema }}%'
        AND NOT LOWER(SCHEMA_NAME) = 'dry';
      COMMIT;
    {% endset %}
    {% do run_query(query) %}

  {% endif %}  

{% endmacro %}

{% macro clean_up_all_test() %}
  {% if target.name == 'prod' %}
    {{ log('Do not do that.', True) }}

  {% else %}
    {{ log('Cleaning up all test branches...', True) }}

    {%- call statement('get_schemata', fetch_result=True) %}
      SELECT SCHEMA_NAME 
      FROM INFORMATION_SCHEMA.SCHEMATA
      WHERE SCHEMA_NAME ILIKE 'TEST_%';
    {%- endcall -%}

    {%- set schema_list = load_result('get_schemata') -%}
    {%- set schemas = schema_list['data'] | map(attribute=0) | list %}
    
    {% for schema in schemas %}
      {{ clean_up_schema(schema) }}
    {% endfor %}

    {{ log('Deleting all test from delete_bookmarks...', True) }}
    {% set query %}
      DELETE FROM {{ var('delete_bookmarks_table') }}
      WHERE SCHEMA_NAME ILIKE 'TEST_%'
        AND NOT LOWER(SCHEMA_NAME) = 'dry';
      COMMIT;
    {% endset %}
    {% do run_query(query) %}

  {% endif %}  
{% endmacro %}