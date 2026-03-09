CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }}
USING TEMPLATE(
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@{{ params.stage }}/{{ params.file }}'
        , FILE_FORMAT => '{{ params.file_format_name }}'
        , IGNORE_CASE => TRUE
      )
    )
);
