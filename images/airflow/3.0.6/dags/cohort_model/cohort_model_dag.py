import os

from airflow.sdk import dag, chain, task_group, Param, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam
from cohort_model.generate_aggregate_curves_task_group import generateAggregateRetentionCurves
from cohort_model.runtime_queries_task_group import runtimeQueries


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': slack_param(),
        'database': Param('MASALA', type='string'),
        'schema': Param('YARROW', type='string'),
        'runtime_schema': Param('COHORT_MODEL_TEMP', type='string'),
        'refresh_cohort_mix_projections': Param(False, type='boolean'),
        'refresh_retention_curves': Param(False, type='boolean'),
        'lookback_adjustment_window': Param(LOOKBACK_ADJUSTMENT_WINDOW, type='number'),
        'longtail_weekly_retention_multiplier': Param(LONGTAIL_WEEKLY_RETENTION_MULTIPLIER, type='number'),
        'meals_per_order_d2c_not_holiday': mealsPerOrderAssumptionsParam('D2C Not Holiday', 6.200),
        'meals_per_order_d2c_holiday': mealsPerOrderAssumptionsParam('D2C Holiday', 5.680),
        'meals_per_order_sale': mealsPerOrderAssumptionsParam('Sale', 6.000),
        'meals_per_order_amazon': mealsPerOrderAssumptionsParam('Amazon', 5.200),
        'meals_per_order_costco': mealsPerOrderAssumptionsParam('Costco', 5.000),
        'meals_per_order_other': mealsPerOrderAssumptionsParam('Other', 5.200),
        'six_week_attach_rates_d2c_not_holiday': sixWeekAttachRateParam('D2C Not Holiday', [0.721, 0.188, 0.026, 0.010, 0.010, 0.000]),
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
  create_temp_schema = SQLExecuteQueryOperator(
    task_id='create_temp_schema',
    conn_id='snowflake',
    sql='CREATE SCHEMA IF NOT EXISTS {{ params.temp_schema }};'
  )

  create_agg_retention_curves = generateAggregateRetentionCurves()

  create_temp_queries = runtimeQueries(default_queries=['historical_meal_orders', 'actual_oven_sales'])

  #snapshot_inputs = snapshotSnowflakeToS3()

  drop_temp_schema = SQLExecuteQueryOperator(
    task_id='drop_temp_schema',
    conn_id='snowflake',
    sql='DROP SCHEMA {{ params.schema }};'
  ).as_teardown(setups=create_temp_schema)
    

  chain(create_temp_schema, [create_temp_queries, create_agg_retention_curves], drop_temp_schema)

cohortModel()