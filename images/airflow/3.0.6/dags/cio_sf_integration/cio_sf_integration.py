
import os
from typing import Dict, List
from pendulum import duration

from airflow.sdk import dag, task, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import bad_boy, good_boy, slack_param

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    # Move CIO data into Snowflake tables every day at 7am 
    schedule=CronTriggerTimetable('0 7 * * *', timezone='America/Chicago'),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': duration(seconds=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': duration(minutes=5),
    },
    tags=['internal', 'data-integration', 'chili'],
    params={
        'channel_name': slack_param()
    },
    template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates'
)
def customerIOSnowflakeIntegration():
  '''Customer IO - Snowflake Integration 
  Description: We move Customer IO data from S3 buckets to a Snowflake external stage. 
    Now, we want to copy data from the external stage (customerio_data_v2.s3_files) into 
    the tables we created within customerio_data_v2. 

  Schedule: Daily

  Dependencies:

  Variables:

  '''
  # Subset of Customer IO table schemas that we want to load into Snowflake 
  TABLE_NAMES = ['broadcasts', 'campaigns', 'campaign_actions', 'deliveries', 'metrics', 'outputs', 'people', 'subjects']
  
  #### Custom Task Definitions
  @task()
  def processTableNames(tables: List[str]) -> List[Dict[str, str]]:
    expanded_args = []

    for table in tables: 
        expanded_args.append({'table': f'CHILI_V2.{table}', 'pattern': f'.*{table}.*.parquet'})

    return expanded_args 

  #### Task Instances
  cio_stage = SQLExecuteQueryOperator(
    task_id='createCioStage', 
    conn_id='snowflake', 
    sql='create_stage.sql',
    params={
      'database': 'MASALA',
      'schema': 'CHILI_V2',
      'stage': 'cio_stage',
      'url': 's3://tovala-data-customerio/',
      'storage_integration': 'CIO_STORAGE_INTEGRATION',
      'file_format': 'parquet',
    },
  )
  
  process_tables = processTableNames(TABLE_NAMES)

  copy_tables = CopyFromExternalStageToSnowflakeOperator.partial(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage='MASALA.CHILI_V2.cio_stage',
    file_format="(TYPE = 'parquet')",
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'
    ).expand_kwargs(process_tables)

  chain([cio_stage, process_tables], copy_tables)

# DAG call
customerIOSnowflakeIntegration()