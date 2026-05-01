COPY INTO @{{ params.database}}.{{ params.schema }}.{{ params.stage }}/{{ run_id }}/runtime_{{ params.table }}.snappy.parquet
FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".{{ params.table }}
FILE_FORMAT = (TYPE = {{ params.file_format }})
SINGLE = TRUE
HEADER = TRUE
;