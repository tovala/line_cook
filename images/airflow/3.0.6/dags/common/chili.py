from airflow.providers.snowflake.utils.common import enclose_param
from airflow.sdk import Param

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

  return params_dict


def _external_stage_select(columns, where_clause, schema, stage, prefix):
  return f'''SELECT
    {columns}
  FROM @{schema}.{'/'.join([stage, prefix]) if prefix else stage}
  {'WHERE' + where_clause if where_clause else ''}
  '''

def generate_create_chili_table_query(table, columns, stage, run_id, **kwargs):
  database = kwargs.get('database', 'MASALA')
  schema = kwargs.get('schema', 'CHILI_V2')

  where_clause = kwargs.get('where_clause')

  clean_run_id = run_id.replace(':', '-').replace('+', '-').replace('.', '-')

  return f'''CREATE OR REPLACE TABLE {database}.{schema}.{table} AS (
    {_external_stage_select(columns, where_clause, schema, stage, clean_run_id)}
  );
  '''

def generate_copy_into_chili_query(table, columns, stage, **kwargs):
  database = kwargs.get('database', 'MASALA')
  schema = kwargs.get('schema', 'CHILI_V2')
  prefix = kwargs.get('prefix')

  where_clause = kwargs.get('where_clause')
  pattern = kwargs.get('pattern')
  file_format = kwargs.get('file_format', 'JSON')

  return f'''COPY INTO {database}.{schema}.{table} FROM (
    {_external_stage_select(columns, where_clause, schema, stage, prefix)}
  )
  {'PATTERN=' + enclose_param(pattern) if pattern else ''}
  FILE_FORMAT= (TYPE = '{file_format}');
  '''
