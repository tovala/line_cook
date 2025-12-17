import datetime

import json
import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from typing import Any, Dict, List
from pendulum import duration
from common.slack_notifications import bad_boy, good_boy

from airflow.sdk import dag, task, chain, Variable
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.slack.notifications.slack import SlackNotifier
from common.slack_notifications import bad_boy, good_boy
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=CronTriggerTimetable("45 7 * * *", timezone="America/Chicago"),
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
  @task 
  def getTableNames() -> List[str]:
    '''
    Retrieve list of table names from Snowflake to then copy data into based on S3 file name.
    
    Args:
    None

    Output:
    List of table names. 
    '''
    table_names = []
    query = '''SELECT LOWER(table_name) AS table 
            FROM MASALA.information_schema.tables 
            WHERE table_schema='CUSTOMERIO_DATA_V2'; '''

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake')
    sf_connection = sf_hook.get_conn()

    cursor = sf_connection.cursor()

    for table in cursor.execute(query):
      table_names.append(table)
    
    return table_names 
  
  @task 
  def copyData(table_name: List[str]):
    # Copy CIO S3 files into the Snowflake table according to the schema name 
    table = table_name 
    copy_into_tables = SQLExecuteQueryOperator(
      task_id="copy_into_tables", 
      conn_id="snowflake", 
      sql=f'''COPY INTO customerio_data_v2.{table}
              FROM @MASALA.customerio_data_v2.s3_files
              PATTERN='.*{table}.*.parquet'
              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ;;''',
      do_xcom_push=True,
    )
    print(f'Loaded table {table}')
    return copy_into_tables

  tables = getTableNames()
  copyData.expand(table_name=tables)

# DAG call
cio_sf_integration()