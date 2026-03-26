CREATE OR REPLACE TABLE {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".aggregate_{{ params.retention_curves_table }}
AS
  SELECT 
    cohort_data.cohort
    {% for week_num in range(79) %}
    , SUM(retention_curves.week_{{ week_num }} * cohort_data.percentage_of_cohort) AS agg_week_{{ week_num }}
    {% endfor %}
  FROM {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}".combined_cohort_characteristics_data AS cohort_data JOIN {{ params.database }}.{{ params.schema }}.{{ params.retention_curves_table }} AS retention_curves ON cohort_data.curve_definition_id = retention_curves.curve_definition_id
  group by cohort_data.cohort order by 1;