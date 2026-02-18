from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, BranchSQLOperator
from airflow.providers.snowflake.utils.common import enclose_param
from airflow.sdk import chain, Param, task_group


def chili_macros():
  from common.chili import generate_copy_into_chili_query, generate_create_chili_table_query

  return {
    'generate_create': generate_create_chili_table_query,
    'generate_copy_into': generate_copy_into_chili_query
  }

def chili_params(table, stage, columns, storage_integration, s3_url, **kwargs):
  '''
  Generate a Params dict for chili dags. 

  :param table: Name of the destination table
  :param stage: Name of the source external stage
  :param columns: sql string of the columns to bring in
  :param storage_integration: Name of the Snowflake storage integration for this bucket
  :param s3_url: url for the s3 bucket associated with this source and stage
  :param kwargs: optional parameters that can be passed to a chili dag as needed
  '''
  params_dict = {
    'full_refresh': Param(False, type='boolean'),
    'storage_integration': Param(storage_integration, type='string'),
    's3_url': Param(s3_url, type='string'),
    'database': Param('MASALA', type='string'),
    'schema': Param('CHILI_V2', type='string'),
    'table': Param(table, type='string'),
    'stage': Param(stage, type='string'),
    'prefix': Param(None, type=['string', 'null']),
    'columns': Param(columns, type='string'),
    'where_clause': Param(None, type=['string', 'null']),
    'file_format': Param('JSON', type='string'),
    'pattern': Param(None, type=['string', 'null'])
  }

  for param_name, default_value in kwargs.items():

    if isinstance(default_value, bool):
      params_dict[param_name] = Param(default_value, type='boolean')
    else:
      params_dict[param_name] = Param(default_value, type=['string', 'null'])
  

  return params_dict


def _external_stage_select(columns, where_clause, schema, stage, prefix):
  return f'''SELECT
    {columns}
  FROM @{schema}.{'/'.join([stage, prefix]) if prefix else stage}
  {'WHERE' + where_clause if where_clause else ''}
  '''

def generate_create_chili_table_query(database, schema, table, columns, where_clause, stage, run_id):
  clean_run_id = run_id.replace(':', '-').replace('+', '-').replace('.', '-')

  return f'''CREATE OR REPLACE TABLE {database}.{schema}.{table} AS (
    {_external_stage_select(columns, where_clause, schema, stage, clean_run_id)}
  );
  '''

def generate_copy_into_chili_query(database, schema, table, columns, where_clause, stage, prefix, pattern, file_format):
  return f'''COPY INTO {database}.{schema}.{table} FROM (
    {_external_stage_select(columns, where_clause, schema, stage, prefix)}
  )
  {'PATTERN=' + enclose_param(pattern) if pattern else ''}
  FILE_FORMAT= (TYPE = '{file_format}');
  '''

@task_group(group_id='chili_load')
def chiliLoad():
  table_exists = BranchSQLOperator(
    task_id='check_table_exists',
    conn_id='snowflake',
    sql='check_table_existence.sql',
    follow_task_ids_if_true=f'chili_load.chili_copy_into',
    follow_task_ids_if_false=f'chili_load.create_chili_table',
  )

  create_stage = SQLExecuteQueryOperator(
    task_id='create_stage',
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  create_chili_table = SQLExecuteQueryOperator(
    task_id='create_chili_table',
    conn_id='snowflake',
    sql='{{ generate_create(params.database, params.schema, params.table, params.columns, params.where_clause, params.stage, run_id) }}'
  )

  chili_copy_into = SQLExecuteQueryOperator(
    task_id='chili_copy_into',
    conn_id='snowflake',
    sql='{{ generate_copy_into(params.database, params.schema, params.table, params.columns, params.where_clause, params.stage, params.prefix, params.pattern, params.file_format) }}',
    trigger_rule='none_failed' # should run in the case when the upstream create_chili_table task is skipped by branching
  )

  chain([table_exists, create_stage], create_chili_table, chili_copy_into)