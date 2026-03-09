import os

from airflow.sdk import dag, chain, task_group, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@task_group(group_id='cohort_mix_projections_load')
def cohortMixProjectionsLoad():
  '''

  Description:
  Schedule: TBD

  Dependencies:

  Variables:

  '''

  create_cohort_mix_projections_table = SQLExecuteQueryOperator(
    task_id='create_cohort_mix_projections_table', 
    conn_id='snowflake', 
    sql='queries/retention_curves/create_table_from_file.sql',
    params={
      'table': 'COHORT_MIX_PROJECTIONS',
      'file': 'cohort_model_cohort_mix_projections.csv'
    },
  ) # could we generate this on the fly?

  copy_cohort_mix_projections = CopyFromExternalStageToSnowflakeOperator(
    task_id='copy_cohort_mix_projections', 
    snowflake_conn_id='snowflake',
    stage='masala.mugwort.cohort_model_inputs_stage',
    file_format='mugwort.s3_csv_format',
    table='MUGWORT.COHORT_MIX_PROJECTIONS',
    files=['cohort_model_cohort_mix_projedctions.csv'],
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
  )
  


  chain(create_cohort_mix_projections_table, copy_cohort_mix_projections)