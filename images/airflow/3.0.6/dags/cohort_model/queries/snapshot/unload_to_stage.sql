COPY INTO @{{ params.database}}.{{ params.schema }}.{{ params.stage }}/{{ run_id }}/
FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".{{ params.table }}
FILE_FORMAT = (TYPE = {{ params.file_format }});