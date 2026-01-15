/* 
To use the dynamic_pivot_generator you will need:
  * source_pivot - the column from the table that contains the values that you want to pivot by (ex: cook_event_type)
  * source_table - the table that contains the source_pivot column 
  * (OPTIONAL) source_where_clause - a where clause to limit the source table

  * window_function - SUM, COUNT etc. 
  * pivot_column - the column you are pivoting by
  * pivot_value_field - the value that you are pivoting by
  * pivot_suffix - the suffix to add to the new column with the pivoted value
  * (OPTIONAL) else_clause - for the window function
  * (OPTIONAL) distinctness - set to distinct if you'd like the window function on distinct values
*/

{% macro generate_pivot_list(source_pivot, source_table, source_where_clause='1=1') %}
  {%- call statement('my_statement', fetch_result=True) -%}
    SELECT DISTINCT {{ source_pivot }}
    FROM {{ ref(source_table) }} 
    WHERE {{ source_where_clause }}
  {%- endcall -%}
  {%- set my_var = load_result('my_statement')['data'] -%}
  {{ return(my_var) }}
{% endmacro %}

{% macro generate_group_by(window_function, pivot_column, pivot_field_name, pivot_value_field, pivot_suffix, else_clause='ELSE 0', distinctness='') %}
  , {{ window_function}}({{ distinctness }} CASE WHEN {{ pivot_column }} = '{{ pivot_field_name }}' THEN {{ pivot_value_field }} {{ else_clause }} END) AS {{ pivot_field_name }}_{{ pivot_suffix }}
{% endmacro %}

{% macro dynamic_pivot_generator(source_pivot, source_table, window_function, pivot_column, pivot_value_field, pivot_suffix, source_where_clause='1=1', else_clause='ELSE 0', distinctness='') %}
  {%- set pivot_fields = generate_pivot_list(source_pivot, source_table, source_where_clause)-%}
  {%- for v in pivot_fields -%}
    {%- set pivot_field_name = v[0] -%}
    {{ generate_group_by(window_function, pivot_column, pivot_field_name, pivot_value_field, pivot_suffix, else_clause, distinctness) }}
  {%- endfor -%}
{% endmacro %}