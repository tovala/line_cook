import datetime

from typing import Any, Dict, List
from pendulum import duration

from airflow.sdk import dag, task, chain, Variable, Connection, XComArg
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import bad_boy, good_boy


@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    # Move CIO data into Snowflake tables every day at 7am 
    schedule=CronTriggerTimetable("0 7 * * *", timezone="America/Chicago"),
    catchup=False,
    default_args={
       "retries": 2,
        "retry_delay": duration(seconds=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=5),
    },
    tags=['internal', 'data-integration'],
    params={
        "channel_name": "#team-data-notifications"
    }
)
def cio_sf_integration():
  '''Customer IO - Snowflake Integration 
  Description: We move Customer IO data from S3 buckets to a Snowflake external stage. 
    Now, we want to copy data from the external stage (customerio_data_v2.s3_files) into 
    the tables we created within customerio_data_v2. 

  Schedule: Daily

  Dependencies:

  Variables:

  '''
  cio_stage = SQLExecuteQueryOperator(
      task_id='create_cio_stage', 
      conn_id='snowflake', 
      sql='queries/create_stage.sql',
      params={
        'parent_database': 'MASALA',
        'schema_name': 'CHILI_V2',
        'stage_name': 'cio_stage',
        'url': 's3://tovala-data-customerio/',
        'storage_integration': 'CIO_STORAGE_INTEGRATION',
        'file_type': 'parquet',
      },
  )

  # Subset of Customer IO table schemas that we want to load into Snowflake 
  TABLE_NAMES = ['broadcasts', 'campaigns', 'campaign_actions', 'deliveries', 'metrics', 'outputs', 'people', 'subjects']

  @task()
  def processTableNames(tables: List[str]) -> List[Dict[str, str]]:
    expanded_args = []

    for table in tables: 
        expanded_args.append({'table': f'CHILI_V2.{table}', 'pattern': f'.*{table}.*.parquet'})

    return expanded_args 

  process_tables = processTableNames(TABLE_NAMES)

  CopyFromExternalStageToSnowflakeOperator.partial(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage='MASALA.CHILI_V2.cio_stage',
    file_format="(TYPE = 'parquet')",
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
    ).expand_kwargs(process_tables)

# DAG call
cio_sf_integration()