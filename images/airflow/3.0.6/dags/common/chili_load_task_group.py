from typing import Dict, List

from airflow.sdk import task_group, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.snowflake_custom_operators import CopyTransformFromExternalStageToSnowflakeOperator

@task_group()
def loadIntoChili(file_type: str, stage_name: str, table: str, s3_url: str, sf_storage_integration: str, columns_array: List[str], **copy_table_args):
  
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

  create_table = SQLExecuteQueryOperator(
    task_id=f'create_table',
    conn_id='snowflake',
    sql=f'''
    CREATE TABLE IF NOT EXISTS CHILI_V2.{ table } (
      {' VARIANT, '.join(columns_array) + ' VARIANT'}
    );
    ''',
  )

  copy_table = CopyTransformFromExternalStageToSnowflakeOperator(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage=f'MASALA.CHILI_V2.{stage_name}',
    schema='CHILI_V2',
    table=table,
    file_format=f"(TYPE = '{file_type}')",
    columns_array=columns_array,
    **copy_table_args
  )

  chain([create_external_stage, create_table], copy_table)

  
