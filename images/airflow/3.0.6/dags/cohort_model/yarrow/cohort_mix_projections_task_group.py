import os

from airflow.sdk import chain, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.common_tasks  import getDagParams


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@task_group(group_id='cohort_mix_projections')
def cohortMixProjections():
  '''

  Description:
  Schedule: TBD

  Dependencies:

  Variables:

  '''

  dag_params = getDagParams()

  create_cohort_mix_projections_table = SQLExecuteQueryOperator(
    task_id='create_cohort_mix_projections_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
    params={
      'table': 'COHORT_MIX_PROJECTIONS',
      'file': 'cohort_model_cohort_mix_projections.csv'
    },
  ) # could we generate this on the fly?

  copy_cohort_mix_projections = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_projections', 
    snowflake_conn_id='snowflake',
    stage=f'{dag_params['database']}.{dag_params['schema']}.{dag_params['stage']}',
    file_format=f'{ dag_params['database']}.{dag_params['schema']}.{dag_params['file_format_name']}',
    table=f'{dag_params['database']}.{dag_params['schema']}.COHORT_MIX_PROJECTIONS',
    files=['cohort_model_cohort_mix_projections.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )
  


  chain(create_cohort_mix_projections_table, copy_cohort_mix_projections)