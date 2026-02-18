{% if params.full_refresh %}
  SELECT 0;
{% else %}
  SELECT COUNT(*) 
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = UPPER('{{ params.schema }}') AND TABLE_NAME = UPPER('{{ params.table }}');
{% endif %}
