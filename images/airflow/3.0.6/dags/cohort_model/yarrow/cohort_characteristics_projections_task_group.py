
from airflow.sdk import chain, task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.extended_operators import TemplatedCopyFromExternalStageToSnowflakeOperator


@task_group(group_id='cohort_mix_projections')
def cohortCharacteristicsProjections():
  '''

  Description:
  Schedule: TBD

  Dependencies:

  Variables:

  '''

  @task(task_id='set_up_characteristics', multiple_outputs=True)
  def setUpCharacteristics(**context):
    dag_params = context['params']
    cohort_characteristics = dag_params.get('cohort_characteristics')
    database = dag_params.get('database')
    schema = dag_params.get('schema')

    create_table_params = []
    external_copy_args = []

    for char_name in cohort_characteristics:
      # formatted names that are used in both table creation and external copy
      file = f'retention_curve_characteristics/{char_name}/cohort_mix.csv'
      table_name = f'{char_name}_mix'
      # add params for create table
      create_table_params.append({ 'table': table_name, 'file': file})
      #add args for external copy
      external_copy_args.append(
        {
          'table': f'{database}.{schema}.{table_name}',
          'files': [file]
        }
      )
    
    return { 
      'create_table_params': create_table_params,
      'external_copy_args': external_copy_args
    }

  characteristic_cohort_mix_details = setUpCharacteristics()
  create_cohort_mix_projections_table = SQLExecuteQueryOperator(
    task_id='create_cohort_mix_projections_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
  ).expand_kwargs(params=characteristic_cohort_mix_details['create_table_params'])

  copy_cohort_mix_projections = TemplatedCopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_projections', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  ).expand_kwargs(characteristic_cohort_mix_details['external_copy_args'])
  


  chain(create_cohort_mix_projections_table, copy_cohort_mix_projections)