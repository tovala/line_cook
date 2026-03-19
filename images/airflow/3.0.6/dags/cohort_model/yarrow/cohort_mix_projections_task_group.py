
from typing import Any, Dict

from airflow.sdk import chain, task_group, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.extended_operators import TemplatedCopyFromExternalStageToSnowflakeOperator


@task_group(group_id='cohort_mix_projections')
def cohortMixProjections():
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
  
  @task(task_id='return_details')
  def returnDetails(details_dict: Dict[str, Any], type_key: str):
    return details_dict[type_key]

  characteristic_cohort_mix_details = setUpCharacteristics()

  create_table_params = returnDetails(characteristic_cohort_mix_details, 'create_table_params')
  create_cohort_mix_projections_table = SQLExecuteQueryOperator.partial(
    task_id='create_cohort_mix_projections_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
  ).expand(params=create_table_params)

  external_copy_args = returnDetails(characteristic_cohort_mix_details, 'external_copy_args')
  copy_cohort_mix_projections = TemplatedCopyFromExternalStageToSnowflakeOperator.partial(
    task_id='copy_cohort_mix_projections', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  ).expand_kwargs(external_copy_args)

  create_cohort_mix_unknown_ratio_table = SQLExecuteQueryOperator(
    task_id='create_cohort_mix_unknown_ratio_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
    params={
      'table': 'cohort_mix_unknown_ratio',
      'file': 'retention_curve_characteristics/cohort_mix_unknown_ratio.csv'
    },
  )

  copy_cohort_mix_unknown_ratio_table = TemplatedCopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_unknown_ratio', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    table='{{ params.database }}.{{ params.schema }}.COHORT_MIX_UNKNOWN_RATIO',
    files=['retention_curve_characteristics/cohort_mix_unknown_ratio.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )


  chain(characteristic_cohort_mix_details, [create_table_params, external_copy_args])
  chain(create_table_params, create_cohort_mix_projections_table)
  chain(external_copy_args, copy_cohort_mix_projections)
  chain(create_cohort_mix_projections_table, copy_cohort_mix_projections)

  chain(create_cohort_mix_unknown_ratio_table, copy_cohort_mix_unknown_ratio_table)