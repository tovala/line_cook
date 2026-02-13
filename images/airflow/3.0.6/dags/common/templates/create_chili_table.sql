{% set where_clause = params.where_clause if params.where_clause is defined else None %}

CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }} AS (
      {{external_stage_select(params.columns, where_clause, params.stage, run_id)}}
);