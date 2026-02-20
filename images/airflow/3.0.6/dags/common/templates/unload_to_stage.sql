COPY INTO @{{ params.stage }}/{{ params.prefix }}
FROM {{ params.table }}
FILE_FORMAT = (TYPE = PARQUET);