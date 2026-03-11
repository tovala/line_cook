CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }}
USING TEMPLATE(
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@{{ params.database }}.{{ params.schema }}.{{ params.stage }}/{{ params.file }}'
        , FILE_FORMAT => '{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}'
        , IGNORE_CASE => TRUE
      )
    )
);
