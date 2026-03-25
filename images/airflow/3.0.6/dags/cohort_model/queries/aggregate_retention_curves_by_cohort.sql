CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".aggregate_{{ params.retention_curves_table }}
AS
  SELECT 
    cohort_data.cohort
    , retention_curves.curve_definition_id
    ,COALESCE(cohort_data.percentage_of_cohort, cohort_data.known_unknown_multiplier) AS curve_multiplier -- if known characteristics, use the calculated percent of cohort, else use unkown characteristics multiplier
    {% for week_num in range(79) %}
    , (retention_curves.week_{{ week_num }} * curve_multiplier) AS agg_week_{{ week_num }}
    {% endfor %}
  FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".combined_cohort_characteristics_data AS cohort_data JOIN {{ params.database }}.{{ params.schema }}.{{ params.retention_curves_table }} AS retention_curves ON cohort_data.curve_definition_id = retention_curves.curve_definition_id;