from typing import List, Dict
import ast
import polars as pl
import numpy as np

from airflow.sdk import task_group, chain, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.sql_operator_handlers import fetch_results_array


def fetch_all_future_term_cohort_age(cursor):
  cohort_future_term_age_dict = {}
  results = cursor.fetchall()
  for row in results:
    key = row[0]
    val_array = ast.literal_eval(row[1])
    cohort_future_term_age_dict |= {key: val_array}

  return cohort_future_term_age_dict

@task_group(group_id='order_projections')
def orderProjections() -> None:

  @task
  def computeOrderProjections(projection_terms_array: List[int], cohort_age_dict: Dict[str, List[int]], **context):
    dag_params = context['params']
    database = dag_params.get('database')
    runtime_schema = dag_params.get('runtime_schema_prefix') + '_' + context['run_id']
    lookback_window = dag_params.get('lookback_adjustment_window')
    longtail_value = dag_params.get('longtail_weekly_retention_multiplier')

    # Set up dataframes from local data
    projection_terms_vector = pl.Series('projection_terms', projection_terms_array)


    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Set Up dataframes from Snowflake Queries

    # fetch aggregate order retention curve generated for this run as a DataFrame
    agg_order_retention_curves_arrow_table = cursor.execute(f'''SELECT *
    FROM { database }."{ runtime_schema }".AGGREGATE_ORDER_RETENTION_CURVES;
    ''').fetch_arrow_all()

    agg_order_retention_curves = pl.from_arrow(agg_order_retention_curves_arrow_table)

    # fetch inital order values by cohort
    init_order_val_arrow_table = cursor.execute(f'''SELECT 
      cohort
      ,order_count AS init_order_val 
    FROM { database }."{ runtime_schema }".HISTORICAL_MEAL_ORDERS
    WHERE term_id = cohort
    UNION
    SELECT 
      cohort
      ,init_order_val 
    FROM { database }."{ runtime_schema }".FUTURE_COHORT_INITIAL_ORDER_PREDICTIONS
    ORDER BY cohort;
    ''').fetch_arrow_all()

    inital_order_values_by_cohort_matrix = pl.from_arrow(init_order_val_arrow_table)

    # fetch total order count within lookback window
    lookback_order_sum_arrow_table = cursor.execute(f'''WITH lookback as (
    SELECT 
      *
    FROM { database }."{ runtime_schema}".historical_meal_orders qualify ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY term_id desc)<= { lookback_window })
    SELECT 
        cohort
        ,CASE WHEN COUNT(term_id) = { lookback_window } 
          THEN SUM(order_count)
          ELSE NULL 
        END AS actual_order_counts_total
    FROM lookback GROUP BY cohort ORDER BY cohort;
    ''').fetch_arrow_all()

    lookback_order_sum_by_cohort_matrix = pl.from_arrow(lookback_order_sum_arrow_table)

    # get retention curve values based on cohort age
    all_cohort_projections = []
    column_names = [f'TERM_{t}' for t in projection_terms_array]
    for cohort, age_array in cohort_age_dict.items():

      projections = []
      cohort_retention_curve = agg_order_retention_curves.filter(pl.col('COHORT') == int(cohort))

      try:
        initial_order_count = inital_order_values_by_cohort_matrix.filter(pl.col('COHORT') == int(cohort)).item(0, 'INIT_ORDER_VAL')
      except IndexError:
        initial_order_count = 0
      
      for age in age_array:
        if age < 0:
          projections.append(None)
        elif age <= 78:
          if cohort_retention_curve.is_empty():
            proj = -1
          else:
            proj = cohort_retention_curve.item(0, f'AGG_WEEK_{ age }') * initial_order_count
          projections.append(proj) 
        else:
          if cohort_retention_curve.is_empty():
            proj = -1
          else:
            proj = (agg_order_retention_curves.item(0, 'AGG_WEEK_78') * (longtail_value ** (age - 78))) * initial_order_count
          projections.append(proj)

      cohort_row = {'cohort': cohort, **dict(zip(column_names, projections))}
      all_cohort_projections.append(cohort_row)

    

    order_projections_matrix = pl.from_dicts(all_cohort_projections)

    print(order_projections_matrix)
    


  projection_terms_array = SQLExecuteQueryOperator(
    task_id='get_projection_terms',
    conn_id='snowflake',
    sql='queries/dataframes/prediction_terms_vector.sql',
    handler=fetch_results_array
  )

  cohort_age_dict = SQLExecuteQueryOperator(
    task_id='get_cohort_age_dict',
    conn_id='snowflake',
    sql='queries/dataframes/future_term_cohort_age_matrix.sql',
    handler=fetch_all_future_term_cohort_age
  )

  compute_order_projections = computeOrderProjections(projection_terms_array.output, cohort_age_dict.output)

  chain(projection_terms_array, cohort_age_dict, compute_order_projections)
    

  
  
