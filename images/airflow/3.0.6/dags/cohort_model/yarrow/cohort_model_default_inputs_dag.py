import os

from airflow.sdk import dag, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.yarrow.retention_curves_task_group import retentionCurves
from cohort_model.yarrow.extended_monthly_oven_sales_predictions_task_group import extendedMonthlyOvenSalesPredictions
from cohort_model.yarrow.cohort_mix_projections_task_group import cohortMixProjections


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model_yarrow_tables',
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
      'stage': Param('cohort_model_inputs_stage', type='string'),
      's3_url': Param('s3://tovala-data-cohort-model/inputs/', type='string'),
      'storage_integration': Param('COHORT_MODEL_STORAGE_INTEGRATION', type='string'),
      'file_format': Param('CSV', type='string'),
      'file_format_name': Param('yarrow_csv_file_format', type='string')
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def cohortModelDefaultInputs():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  create_file_format = SQLExecuteQueryOperator(
    task_id='create_file_format',
    conn_id='snowflake',
    sql='create_file_format.sql',
    params={
      'file_format_options': 'PARSE_HEADER = true TRIM_SPACE = true ERROR_ON_COLUMN_COUNT_MISMATCH = false'
    }
  )

  create_cohort_model_inputs_stage = SQLExecuteQueryOperator(
    task_id='create_cohort_model_inputs_stage', 
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  create_retention_curve_tables = retentionCurves()

  create_extended_sales_prediction_table = extendedMonthlyOvenSalesPredictions(
    table='extended_monthly_oven_sales_predictions',
    table_columns_file='queries/extended_monthly_sales_predictions/table_columns.sql'
  )

  create_cohort_mix_projections = cohortMixProjections()
  


  chain(create_file_format, create_cohort_model_inputs_stage, [create_retention_curve_tables, create_extended_sales_prediction_table, create_cohort_mix_projections])

    


cohortModelDefaultInputs()