import os

from airflow.sdk import dag, chain, task_group, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]


@task_group(group_id='generate_aggregate_retention_curves')
def generateAggregateRetentionCurves():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''

  create_characteristic_data_table = SQLExecuteQueryOperator(
    task_id='create_characteristic_data_table',
    conn_id='snowflake',
    sql='queries/combined_cohort_characteristics_data.sql'
  )

  create_agg_meal_retention_curves_table = SQLExecuteQueryOperator(
    task_id='create_agg_meal_retention_curves_table',
    conn_id='snowflake',
    sql='queries/aggregate_retention_curves_by_cohort.sql',
    params={
      'retention_curves_table': 'meal_retention_curves'
    }
  )

  create_agg_order_retention_curves_table = SQLExecuteQueryOperator(
    task_id='create_agg_order_retention_curves_table',
    conn_id='snowflake',
    sql='queries/aggregate_retention_curves_by_cohort.sql',
    params={
      'retention_curves_table': 'order_retention_curves'
    }
  )

  chain(create_characteristic_data_table, [create_agg_order_retention_curves_table, create_agg_meal_retention_curves_table])