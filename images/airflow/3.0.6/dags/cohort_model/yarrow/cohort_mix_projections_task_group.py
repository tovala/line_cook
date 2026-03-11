
from airflow.sdk import chain, task_group
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

  create_cohort_mix_projections_table = SQLExecuteQueryOperator(
    task_id='create_cohort_mix_projections_table', 
    conn_id='snowflake', 
    sql='queries/create_table_from_file.sql',
    params={
      'table': 'COHORT_MIX_PROJECTIONS',
      'file': 'cohort_model_cohort_mix_projections.csv'
    },
  ) # could we generate this on the fly?

  copy_cohort_mix_projections = TemplatedCopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_projections', 
    snowflake_conn_id='snowflake',
    stage='{{ params.database }}.{{ params.schema }}.{{ params.stage }}',
    file_format='{{ params.database }}.{{ params.schema }}.{{ params.file_format_name }}',
    table='{{ params.database }}.{{ params.schema }}.COHORT_MIX_PROJECTIONS',
    files=['cohort_model_cohort_mix_projections.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )
  


  chain(create_cohort_mix_projections_table, copy_cohort_mix_projections)