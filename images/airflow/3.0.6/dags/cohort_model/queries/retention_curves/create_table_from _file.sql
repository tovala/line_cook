CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }}
USING TEMPLATE(
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => {{ params.stage }}
        , FILE_FORMAT => ( TYPE = {{ params.file_format }} )
        , FILES => ( '{{ params.file }}' )
        , IGNORE_CASE => TRUE
      )
    )
)
