import os

from airflow.sdk import dag, chain, task_group, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='retention_curves',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
      'channel_name': slack_param(),
      'database': Param('MASALA', type='string'),
      'schema': Param('MUGWORT', type='string'),
      'stage': Param('retention_curves_stage', type='string'),
      's3_url': Param('s3://tovala-data-cohort-model/inputs/retention_curves', type='string'),
      'storage_integration': Param('COHORT_MODEL_STORAGE_INTEGRATION', type='string'),
      'file_format': Param('csv', type='string'),
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def retentionCurves():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  RETENTION_CURVE_INPUTS = [
    {
      'table': 'MUGWORT.MEAL_RETENTION_CURVES',
      'pattern': '.*meal_retention_curves.*.csv'
    },
    {
      'table': 'MUGWORT.ORDER_RETENTION_CURVES',
      'pattern': '.*order_retention_curves.*.csv'
    }
  ]

  retention_curve_source_stage = SQLExecuteQueryOperator(
    task_id='create_retention_curve_stage', 
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  create_meal_retention_curve_table = SQLExecuteQueryOperator.partial(
    task_id='create_meal_retention_curve_table', 
    conn_id='snowflake', 
    sql='queries/retention_curves/create_table_from_file.sql',
    params={
      'table': 'MEAL_RETENTION_CURVES',
      'file': 'cohort_model_meal_retention_curves.csv'
    },
  )

  create_order_retention_curve_table = SQLExecuteQueryOperator.partial(
    task_id='create_order_retention_curve_table', 
    conn_id='snowflake', 
    sql='queries/retention_curves/create_table_from_file.sql',
    params={
      'table': 'ORDER_RETENTION_CURVES',
      'file': 'cohort_model_order_retention_curves.csv'
    },
  )

  copy_meal_retention_table = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_meal_retention_curves', 
    snowflake_conn_id='snowflake',
    stage='MASALA.MUGWORT.retention_curves_stage',
    file_format="(TYPE = 'csv')",
    copy_options='PARSE_HEADER=True',
    table='MUGWORT.MEAL_RETENTION_CURVES',
    files=['cohort_model_meal_retention_curves.csv']
  )

  copy_order_retention_table = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_order_retention_curves', 
    snowflake_conn_id='snowflake',
    stage='MASALA.MUGWORT.retention_curves_stage',
    file_format="(TYPE = 'csv')",
    copy_options='PARSE_HEADER=True',
    table='MUGWORT.MEAL_RETENTION_CURVES',
    files=['cohort_model_order_retention_curves.csv']
  )
  


  chain(retention_curve_source_stage, [create_meal_retention_curve_table, create_order_retention_curve_table])
  chain(create_meal_retention_curve_table, copy_meal_retention_table)
  chain(create_order_retention_curve_table, copy_order_retention_table)

    


retentionCurves()