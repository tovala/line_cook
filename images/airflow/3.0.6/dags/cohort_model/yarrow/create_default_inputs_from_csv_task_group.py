
from typing import Any, Dict

from airflow.sdk import chain, task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.extended_operators import TemplatedCopyFromExternalStageToSnowflakeOperator


@task_group(group_id='cohort_mix_projections')
def defaultInputsFromCSV(table: str, file: str):
  '''

  Description:
  Schedule: TBD

  Dependencies:

  Variables:

  '''
  create_table = SQLExecuteQueryOperator(
    task_id=f'create_{table}',
    conn_id='snowflake',
    sql='queries/create_table_from_file.sql',
    params={
      'table': table,
      'file': file
    }
  )

  copy_csv = TemplatedCopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_unknown_ratio', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    table='{{ params.database }}.{{ params.schema }}.%s' % (table),
    files=[file],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )

  chain(create_table, copy_csv)
