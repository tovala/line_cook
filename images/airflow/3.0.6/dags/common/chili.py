from airflow.operators.python import get_current_context
from airflow.providers.snowflake.utils.common import enclose_param
from airflow.sdk import Param, task

@task(task_id='render_chili_sql', trigger_rule='one_success')
def renderChiliQuery(table_exists, **context):
  '''
  User defined macro to generate the appropriate SQL query for a given chili load.
  
  :param table_exists: boolean indicating whether the destination table already exists in snowflake
  :param columns: sql string of the columns to bring in from the external stage, including any necessary parsing or transformations
  :param context: context object passed from the chili dag, which should include any params needed to generate the query (e.g. stage name, file format, where clause, etc.)
  '''
  print(context)

  dag_params = context['params']
  run_id = context['run_id']

  # Get params if passed to the dag, otherwise use defaults
  full_refresh = dag_params.get('full_refresh', False)
  file_format = dag_params.get('file_format', 'JSON')
  pattern = dag_params.get('pattern')
  stage = dag_params.get('stage')
  prefix = dag_params.get('prefix')

  database = dag_params.get('database', 'MASALA')
  schema = dag_params.get('schema', 'CHILI_V2')
  table = dag_params.get('table')

  columns = dag_params.get('columns')
  where_clause = dag_params.get('where_clause')

  query = ''

  if not table_exists or full_refresh:
    query = f'''
    CREATE OR REPLACE TABLE {database}.{schema}.{table} AS (
      {_external_stage_select(columns, where_clause, stage, run_id)}
    );
    '''

  query = query + f'''COPY INTO {database}.{schema}.{table} FROM (
  SELECT
    {_external_stage_select(columns, where_clause, stage, prefix)}
  )
  {'PATTERN=' + enclose_param(pattern) if pattern else ''}
  FILE_FORMAT={file_format}
  '''

  return query

def chili_params(table, stage, columns, **kwargs):
  '''
  Generate a Params dict for chili dags. 

  :param table: Name of the destination table
  :param stage: Name of the source external stage
  :param columns: sql string of the columns to bring in
  :param kwargs: optional parameters that can be passed to a chili dag as needed
  '''
  params_dict = {
    'table': Param(table, type='string'),
    'stage': Param(stage, type='string'),
    'columns': Param(columns, type='string'),
  }

  for param_name, default_value in kwargs.items():

    if isinstance(default_value, bool):
      params_dict.update({param_name: Param(default_value, type='boolean')})
    else:
      params_dict.update({param_name: Param(default_value, type='string')})
  
  if 'database' not in params_dict:
    params_dict.update({'database': 'MASALA'})
  if 'schema' not in params_dict:
    params_dict.update({'schema': 'CHILI_V2'})

  if 'file_format' not in params_dict:
    params_dict.update({'file_format': 'JSON'})

  return params_dict


def _external_stage_select(columns, where_clause, stage, prefix):
  return f'''SELECT
    {columns}
    FROM @{'/'.join([stage, prefix]) if prefix else stage}
    {'WHERE' + where_clause if where_clause else ''}
  '''
