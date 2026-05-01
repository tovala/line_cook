SELECT table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = '{{ params.runtime_schema_prefix }}_{{ run_id }}';