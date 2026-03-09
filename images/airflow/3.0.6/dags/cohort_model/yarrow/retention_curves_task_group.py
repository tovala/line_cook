import os

from airflow.sdk import chain, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.common_tasks import getDagParams


@task_group(group_id='retention_curves')
def retentionCurves():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  dag_params = getDagParams()

  create_meal_retention_curve_table = SQLExecuteQueryOperator(
    task_id='create_meal_retention_curve_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
    params={
      'table': 'MEAL_RETENTION_CURVES',
      'file': 'retention_curves/cohort_model_meal_retention_curves.csv'
    },
  )

  create_order_retention_curve_table = SQLExecuteQueryOperator(
    task_id='create_order_retention_curve_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
    params={
      'table': 'ORDER_RETENTION_CURVES',
      'file': 'retention_curves/cohort_model_order_retention_curves.csv'
    },
  )

  copy_meal_retention_table = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_meal_retention_curves', 
    snowflake_conn_id='snowflake',
    stage=f'{dag_params['database']}.{dag_params['schema']}.{dag_params['stage']}',
    file_format=f'{ dag_params['database']}.{dag_params['schema']}.{dag_params['file_format_name']}',
    table=f'{dag_params['database']}.{dag_params['schema']}.MEAL_RETENTION_CURVES',
    files=['cohort_model_meal_retention_curves.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )

  copy_order_retention_table = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_order_retention_curves', 
    snowflake_conn_id='snowflake',
    stage=f'{dag_params['database']}.{dag_params['schema']}.{dag_params['stage']}',
    file_format=f'{ dag_params['database']}.{dag_params['schema']}.{dag_params['file_format_name']}',
    table=f'{dag_params['database']}.{dag_params['schema']}.ORDER_RETENTION_CURVES',
    files=['cohort_model_order_retention_curves.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )
  
  chain(create_meal_retention_curve_table, copy_meal_retention_table)
  chain(create_order_retention_curve_table, copy_order_retention_table)
