from typing import Dict, List

from airflow.sdk import task_group, chain, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.sql_operator_handlers import fetch_results_array

@task_group()
def snapshotSnowflakeToS3(
  file_format: str = 'parquet',
  stage: str = 'cohort_model_snapshot_stage',
  s3_url: str = 's3://tovala-data-cohort-model/output/',
  storage_integration: str = 'COHORT_MODEL_STORAGE_INTEGRATION',
):
  '''Task Group that creates an SF external stage and copies all tables from the runtime schema
  into S3.
  '''

  @task()
  def formatParams(table_names_array: List[str]) -> List[Dict[str, str]]:
    return [{
      'table': t,
      'stage': stage,
      's3_url': s3_url,
      'storage_integration': storage_integration,
      'file_format': file_format
      } for t in table_names_array
    ]
    
  cohort_model_snapshot_stage = SQLExecuteQueryOperator(
    task_id='create_cohort_model_snapshot_stage', 
    conn_id='snowflake', 
    sql='create_stage.sql',
    params={
      'stage': stage,
      's3_url': s3_url,
      'storage_integration': storage_integration,
      'file_format': file_format,
    },
  )

  get_table_names = SQLExecuteQueryOperator(
    task_id='get_table_names',
    conn_id='snowflake',
    sql='queries/snapshot/get_temp_tables.sql',
    handler=fetch_results_array
  )

  formatted_params_list = formatParams(get_table_names.output)

  unload_tables = SQLExecuteQueryOperator.partial(
    task_id='unload_table',
    conn_id='snowflake',
    sql='queries/snapshot/unload_to_stage.sql'
  ).expand(params=formatted_params_list)

  chain(cohort_model_snapshot_stage, get_table_names, unload_tables)