from typing import List

from airflow.sdk import task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    
@task_group(group_id='runtime_queries')
def runtimeQueries(default_queries: List[str], override_queries: List[str] = None) -> None:
  create_temp_table_as_select = SQLExecuteQueryOperator(
    task_id='create_characteristic_data_table',
    conn_id='snowflake',
  ).expand(sql=[f'queries/{query}.sql' for query in default_queries])
        
  # TODO: Add S3 override here - maybe add an override param? like a list of models w override - set comparison to standard list to avoid doing both?
  # copy_from_csv = CopyFromExternalStageToSnowflakeOperator.partial(
  #   task_id='copyTable', 
  #   snowflake_conn_id='snowflake',
  #   stage='MASALA.CHILI_V2.cohort_model_input_stage',
  #   file_format="(TYPE = 'csv')",
  #   params={
  #       'table': f'CHILI_V2.{model}',
  #       'pattern': f'.*{model}.*.parquet'
  #   }
  # )