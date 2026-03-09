
from airflow.sdk import chain, task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator



@task_group(group_id='extended_monthly_oven_sales_predictions')
def extendedMonthlyOvenSalesPredictions():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  @task(task_id='setup_copy_into_args', multiple_outputs=True)
  def getDagParams(**context):
    dag_params = context['params']

    return {
      'database': dag_params.get('database'),
      'schema': dag_params.get('schema'),
      'stage': dag_params.get('stage')
    }


  create_table = SQLExecuteQueryOperator(
    task_id='create_table', 
    conn_id='snowflake', 
    sql='create_table.sql',
    params={
      'table': 'extended_monthly_oven_sales_predictions',
      'table_columns_file': 'queries/extended_monthly_sales_predictions/table_columns.sql'
    }
  )

  dag_params = getDagParams()

  copy_from_s3 = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_from_s3', 
    snowflake_conn_id='snowflake',
    stage=f'{dag_params['database']}.{dag_params['schema']}.{dag_params['stage']}',
    file_format="(TYPE = 'CSV')",
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )

  chain([create_table, dag_params], copy_from_s3)