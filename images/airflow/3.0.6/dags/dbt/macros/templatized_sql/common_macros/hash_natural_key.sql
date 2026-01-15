{% macro hash_natural_key() %}
  MD5(
    '' 
    {% for v in varargs %}
      || nullif({{ v }}, null)
    {% endfor %}
    )
{% endmacro %}