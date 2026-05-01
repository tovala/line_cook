from datetime import timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, BranchSQLOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import enclose_param
from airflow.sdk import chain, Param, task, task_group
from snowflake.connector.constants import QueryStatus

_SNOWFLAKE_CONN_ID = 'snowflake'

_RUNNING_QUERY_STATUSES = {
  QueryStatus.RUNNING,
  QueryStatus.QUEUED,
  QueryStatus.QUEUED_REPARING_WAREHOUSE,
  QueryStatus.RESUMING_WAREHOUSE,
  QueryStatus.NO_DATA,
}


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
    'file_format_name': Param(None, type=['string', 'null']),
    'pattern': Param(None, type=['string', 'null'])
  }

  for param_name, default_value in kwargs.items():

    if isinstance(default_value, bool):
      params_dict[param_name] = Param(default_value, type='boolean')
    else:
      params_dict[param_name] = Param(default_value, type=['string', 'null'])
  

  return params_dict


def _external_stage_select(columns, where_clause, database, schema, stage, prefix):
  return f'''SELECT
    {columns}
  FROM @{database}.{schema}.{'/'.join([stage, prefix]) if prefix else stage}
  {'WHERE' + where_clause if where_clause else ''}
  '''

def generate_create_chili_table_query(database, schema, table, columns, where_clause, stage, run_id):
  clean_run_id = run_id.replace(':', '-').replace('+', '-').replace('.', '-')

  return f'''CREATE OR REPLACE TABLE {database}.{schema}.{table} AS (
    {_external_stage_select(columns, where_clause, database, schema, stage, clean_run_id)}
  );
  '''

def generate_copy_into_chili_query(database, schema, table, columns, where_clause, stage, prefix, pattern, file_format, file_format_name):
  if file_format_name:
    file_format_clause = f"FILE_FORMAT = '{database}.{schema}.{file_format_name}'"
  else:
    file_format_clause = f"FILE_FORMAT = (TYPE = '{file_format}')"
  return f'''COPY INTO {database}.{schema}.{table} FROM (
    {_external_stage_select(columns, where_clause, database, schema, stage, prefix)}
  )
  {'PATTERN=' + enclose_param(pattern) if pattern else ''}
  {file_format_clause};
  '''

@task_group(group_id='chili_load')
def chiliLoad(file_format_options=None):
  table_exists = BranchSQLOperator(
    task_id='check_table_exists',
    conn_id=_SNOWFLAKE_CONN_ID,
    sql='check_table_existence.sql',
    follow_task_ids_if_true='chili_load.submit_chili_copy_into',
    follow_task_ids_if_false='chili_load.create_chili_table',
  )

  create_stage = SQLExecuteQueryOperator(
    task_id='create_stage',
    conn_id=_SNOWFLAKE_CONN_ID,
    sql='create_stage.sql'
  )

  if file_format_options is not None:
    create_file_format = SQLExecuteQueryOperator(
      task_id='create_file_format',
      conn_id=_SNOWFLAKE_CONN_ID,
      sql='create_file_format.sql',
      params={'file_format_options': file_format_options}
    )
    create_file_format >> create_stage

  create_chili_table = SQLExecuteQueryOperator(
    task_id='create_chili_table',
    conn_id=_SNOWFLAKE_CONN_ID,
    sql='{{ generate_create(params.database, params.schema, params.table, params.columns, params.where_clause, params.stage, run_id) }}'
  )

  @task(task_id='submit_chili_copy_into', trigger_rule='none_failed', retries=0)
  def submit_chili_copy_into(**context):
    p = context['params']
    sql = generate_copy_into_chili_query(
      database=p['database'], schema=p['schema'], table=p['table'],
      columns=p['columns'], where_clause=p['where_clause'], stage=p['stage'],
      prefix=p['prefix'], pattern=p['pattern'],
      file_format=p['file_format'], file_format_name=p.get('file_format_name'),
    )
    cursor = SnowflakeHook(snowflake_conn_id=_SNOWFLAKE_CONN_ID).get_conn().cursor()
    cursor.execute_async(sql)
    return cursor.sfqid

  @task.sensor(
    task_id='wait_chili_copy_into',
    poke_interval=15,
    timeout=3600,
    mode='reschedule',
    retries=3,
    retry_delay=timedelta(seconds=30),
  )
  def wait_chili_copy_into(query_id):
    status = SnowflakeHook(snowflake_conn_id=_SNOWFLAKE_CONN_ID).get_conn().get_query_status(query_id)
    if status == QueryStatus.SUCCESS:
      return True
    if status in _RUNNING_QUERY_STATUSES:
      return False
    raise AirflowFailException(f"Snowflake query {query_id} ended with status {status.name}")

  @task(
    task_id='cancel_chili_copy_into',
    trigger_rule='one_failed',
    retries=2,
    retry_delay=timedelta(seconds=30),
  )
  def cancel_chili_copy_into(query_id):
    if not query_id:
      return
    cursor = SnowflakeHook(snowflake_conn_id=_SNOWFLAKE_CONN_ID).get_conn().cursor()
    cursor.execute("SELECT SYSTEM$CANCEL_QUERY(%s)", (query_id,))

  query_id = submit_chili_copy_into()
  wait = wait_chili_copy_into(query_id)
  cancel = cancel_chili_copy_into(query_id)
  wait >> cancel

  chain([table_exists, create_stage], create_chili_table, query_id)