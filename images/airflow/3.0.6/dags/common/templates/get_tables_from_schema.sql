SELECT table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = UPPER('{{ params.schema }}');