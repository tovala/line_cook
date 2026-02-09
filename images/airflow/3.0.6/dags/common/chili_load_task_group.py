from typing import Dict, List

from airflow.sdk import task_group, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.snowflake_custom_operators import CopyTransformFromExternalStageToSnowflakeOperator

@task_group()
def loadIntoChili(file_type: str, stage_name: str, s3_url: str, sf_storage_integration: str, copy_table_args: List[Dict[str, str]]):
  create_external_stage = SQLExecuteQueryOperator(
      task_id=f'create_{stage_name}', 
      conn_id='snowflake', 
      sql='create_stage.sql',
      params={
        'parent_database': 'MASALA',
        'schema_name': 'CHILI_V2',
        'stage_name': stage_name,
        'url': s3_url,
        'storage_integration': sf_storage_integration,
        'file_type': file_type,
      },
    )

  copy_tables = CopyTransformFromExternalStageToSnowflakeOperator.partial(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage=f'MASALA.CHILI_V2.{stage_name}',
    file_format=f"(TYPE = '{file_type}')",
  ).expand_kwargs(copy_table_args)

  chain(create_external_stage, copy_tables)

  
