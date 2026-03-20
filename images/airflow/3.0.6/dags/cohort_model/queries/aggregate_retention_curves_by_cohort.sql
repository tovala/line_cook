select cohort_data.cohort
  , retention_curves.curve_definition_id
  ,COALESCE(cohort_data.percentage_of_cohort, cohort_data.known_unknown_multiplier) as curve_multiplier -- if known characteristics, use the calculated percent of cohort, else use unkown characteristics multiplier
  {% for week_num in range(79) %}
  , (retention_curves.week_{{ week_num }} * curve_multiplier) as agg_week_{{ week_num }}
  {% endfor %}
from {{ params.database }}.{{ params.temp_schema }}.combined_cohort_characteristics_data as cohort_data join {{ params.database }}.{{ params.schema }}.{{ params.retention_curves_table }} as retention_curves on cohort_data.curve_definition_id = retention_curves.curve_definition_id;