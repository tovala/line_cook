{% set where_clause = params.where_clause if params.where_clause is defined else None %}
{% set prefix = params.prefix if params.prefix is defined else None %}
{% set pattern = params.pattern if params.pattern is defined else None %}

COPY INTO {{params.database}}.{{params.schema}}.{{params.table}} FROM (
    {{external_stage_select(params.columns, where_clause, params.stage, prefix)}}
  )
  {{}}



CREATE OR REPLACE TABLE {{ params.database }}.{{ params.schema }}.{{ params.table }} AS (
      {{external_stage_select(params.columns, params.stage, prefix, where_clause)}}
);