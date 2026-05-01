COPY INTO @{{ params.database}}.{{ params.schema }}.{{ params.stage }}/{{ run_id }}/runtime_{{ params.table }}.{{ params.file_format }}
FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".{{ params.table }}
FILE_FORMAT = (TYPE = {{ params.file_format }})
SINGLE = TRUE
HEADER = TRUE
;