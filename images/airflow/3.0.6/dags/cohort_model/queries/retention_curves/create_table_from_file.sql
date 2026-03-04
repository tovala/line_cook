USE SCHEMA {{ params.database }}.{{ params.schema }};

CREATE OR REPLACE FILE FORMAT s3_csv_format
  TYPE = csv
  PARSE_HEADER = true;


CREATE OR REPLACE TABLE {{ params.table }}
USING TEMPLATE(
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@{{ params.stage }}'
        , FILE_FORMAT => 's3_csv_format'
        , FILES => ( '{{ params.file }}' )
        , IGNORE_CASE => TRUE
      )
    )
);
