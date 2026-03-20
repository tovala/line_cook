import os

from airflow.sdk import dag, chain, task_group, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='retention_curves',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
      'channel_name': slack_param(),
      'database': Param('MASALA', type='string'),
      'schema': Param('MUGWORT', type='string'),
      'stage': Param('retention_curves_stage', type='string'),
      's3_url': Param('s3://tovala-data-cohort-model/inputs/', type='string'),
      'storage_integration': Param('COHORT_MODEL_STORAGE_INTEGRATION', type='string'),
      'file_format': Param('CSV', type='string'),
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def generateAggregateRetentionCurve():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  # TODO: create table in temp schema from queries/combined_cohort_characteristics_data.sql

  # TODO: create table in temp schema from queries/aggregate_retention_curves_by_cohort.sql

  # TODO: trigger next step in cohort model pipeline

    


generateAggregateRetentionCurve()