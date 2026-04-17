import os
from pendulum import duration

from airflow.sdk import dag, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam
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
        'lookback_adjustment_window': Param(LOOKBACK_ADJUSTMENT_WINDOW, type='number'),
        'longtail_weekly_retention_multiplier': Param(LONGTAIL_WEEKLY_RETENTION_MULTIPLIER, type='number'),
        'meals_per_order_d2c_not_holiday': mealsPerOrderAssumptionsParam('D2C Not Holiday', 6.200),
        'meals_per_order_d2c_holiday': mealsPerOrderAssumptionsParam('D2C Holiday', 5.680),
        'meals_per_order_sale': mealsPerOrderAssumptionsParam('Sale', 6.000),
        'meals_per_order_amazon': mealsPerOrderAssumptionsParam('Amazon', 5.200),
        'meals_per_order_costco': mealsPerOrderAssumptionsParam('Costco', 5.000),
        'meals_per_order_other': mealsPerOrderAssumptionsParam('Other', 5.200),
        'six_week_attach_rates_d2c_non_holiday': sixWeekAttachRateParam('D2C Non-Holiday', [0.721, 0.188, 0.026, 0.010, 0.010, 0.000]),
        'six_week_attach_rates_d2c_holiday': sixWeekAttachRateParam('D2C Holiday', [0.704, 0.109, 0.061, 0.023, 0.029, 0.021]),
        'six_week_attach_rates_sale': sixWeekAttachRateParam('Sale', [0.704, 0.109, 0.061, 0.023, 0.029, 0.021]),
        'six_week_attach_rates_amazon': sixWeekAttachRateParam('Amazon', [0.000, 0.000, 0.200, 0.050, 0.000, 0.000]),
        'six_week_attach_rates_costco': sixWeekAttachRateParam('Costco', [0.000, 0.000, 0.150, 0.100, 0.000, 0.000]),
        'six_week_attach_rates_other': sixWeekAttachRateParam('Other', [0.000, 0.000, 0.200, 0.050, 0.000, 0.000]),
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

  run_order_projections = orderProjections()

  #snapshot_inputs = snapshotSnowflakeToS3()
  
  delete_runtime_schema = SQLExecuteQueryOperator(
    task_id='drop_runtime_schema',
    conn_id='snowflake',
    sql='DROP SCHEMA {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}";'
  ).as_teardown()

  chain(create_runtime_schema, [create_temp_queries, create_agg_retention_curves], run_order_projections, delete_runtime_schema)

    

cohortModel()