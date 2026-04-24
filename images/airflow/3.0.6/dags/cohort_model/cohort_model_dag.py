import os
from pendulum import duration

from airflow.sdk import dag, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.slack_notifications import slack_param
from common.sql_operator_handlers import fetch_single_result, fetch_results_array
from cohort_model.generate_aggregate_curves_task_group import generateAggregateRetentionCurves
from cohort_model.runtime_queries_task_group import runtimeQueries
from cohort_model.order_projections_task_group import orderProjections


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    default_args={
      'retries': 2,
      'retry_delay': duration(seconds=2),
      'retry_exponential_backoff': True,
      'max_retry_delay': duration(minutes=5),
    },
    tags=['internal'],
    params={
        'channel_name': slack_param(),
        'database': Param('MASALA', type='string'),
        'schema': Param('YARROW', type='string'),
        'runtime_schema_prefix': Param('COHORT_MODEL', type='string'),
        'lookback_adjustment_window': Param(5, type='number'),
        'longtail_weekly_retention_multiplier': Param(0.9945, type='number'),
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def cohortModel():
  '''Cohort Model
  Description: 

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  create_runtime_schema = SQLExecuteQueryOperator(
    task_id='create_runtime_schema',
    conn_id='snowflake',
    sql='CREATE SCHEMA IF NOT EXISTS {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}";'
  ).as_setup()

  create_agg_retention_curves = generateAggregateRetentionCurves()

  create_temp_queries = runtimeQueries(default_queries=['historical_meal_orders', 'actual_oven_sales', 'future_cohort_initial_order_predictions', 'cohort_age'])

  start_term = SQLExecuteQueryOperator(
    task_id='get_projection_start_term',
    conn_id='snowflake',
    sql='SELECT MIN(term_id) FROM {{ params.database }}.mugwort.future_terms',
    handler=fetch_single_result
  )

  end_term = SQLExecuteQueryOperator(
    task_id='get_projection_end_term',
    conn_id='snowflake',
    sql='SELECT MAX(term_id) from {{ params.database }}.mugwort.combined_oven_sales',
    handler=fetch_single_result
  )

  projection_terms_array = SQLExecuteQueryOperator(
    task_id='get_projection_terms',
    conn_id='snowflake',
    sql='queries/projection_terms_array.sql',
    handler=fetch_results_array
  )

  cohort_age_matrix = SQLExecuteQueryOperator(
    task_id='get_cohort_age_matrix',
    conn_id='snowflake',
    sql= 'queries/projection_terms_cohort_age_matrix.sql'
  )

  run_order_projections = orderProjections(projection_terms_array.output)

  #snapshot_inputs = snapshotSnowflakeToS3()
  
  delete_runtime_schema = SQLExecuteQueryOperator(
    task_id='drop_runtime_schema',
    conn_id='snowflake',
    sql='DROP SCHEMA {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}";'
  ).as_teardown()

  chain([start_term, end_term], projection_terms_array, run_order_projections)
  chain(create_runtime_schema, [create_temp_queries, create_agg_retention_curves], run_order_projections, delete_runtime_schema)

    

cohortModel()