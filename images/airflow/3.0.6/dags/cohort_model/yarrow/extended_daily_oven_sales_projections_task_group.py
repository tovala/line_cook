
from airflow.sdk import chain, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.extended_operators import TemplatedCopyFromExternalStageToSnowflakeOperator

@task_group(group_id='extended_daily_oven_sales_projections')
def extendedDailyOvenSalesProjections(table: str, table_columns_file: str):
  '''Extended Daily Oven Sales Projections

  Description: long-term oven sales predictions by month for terms beyond the current growth team oven sales predictions.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  create_table = SQLExecuteQueryOperator(
    task_id='create_table', 
    conn_id='snowflake', 
    sql='create_table.sql',
    params={
      'table': table,
      'table_columns_file': table_columns_file
    }
  )


  copy_from_s3 = TemplatedCopyFromExternalStageToSnowflakeOperator(
    task_id='copy_from_s3', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    table='{{ params.database }}.{{ params.schema }}.%s' % (table),
    files=['extended_daily_sales_projections.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE INCLUDE_METADATA = (loaded_at = METADATA$START_SCAN_TIME)'
  )

  chain(create_table, copy_from_s3)