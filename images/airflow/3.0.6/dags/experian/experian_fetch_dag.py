import json
import math
import requests
from requests import HTTPError
from typing import List, Dict, Any

from airflow.sdk import dag, task, task_group, Variable, chain
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

BATCH_SIZE = 4
GET_TOKEN_URL = 'https://us-api.experian.com/oauth2/v1/token'

def fetch_single_result(cursor):
  output, = cursor.fetchone()

  return output

@dag(
  # on_failure_callback=bad_boy,
  # on_success_callback=good_boy,
  # schedule=CronTriggerTimetable("0 3 * * *", timezone="America/Chicago"),
  catchup=False,
  # default_args={
  #     'retries': 2,
  #     'retry_delay': duration(seconds=2),
  #     'retry_exponential_backoff': True,
  #     'max_retry_delay': duration(minutes=5),
  # },
  tags=['internal', 'data-integration'],
  params={
    'channel_name': '',
    'temp_table_prefix': 'experian_customers'
  }
)
def experianExtraction():
  ''' Experian Extraction Pipeline
  Description: Retrieves Experian information for new customers

  Schedule: Daily at 3AM

  Dependencies:

  Variables:

  '''
  # TODO: pull customer data, format, save to temp table in sf - return size of temp table - use size to determine how many batches
  # use a for loop at dag level (this is ok, as long as we have < 1000 iterations, speed is comparable to dynamic task mapping)

  # TODO: SELECT * from brine.experian_customers_temp order by row_number limit {page_size} offset {page_size}*{{ ti.map_index }};  

  # TODO: send batch to experian
  
  create_temporary_table = SQLExecuteQueryOperator(
    task_id='create_temporary_table', 
    conn_id='snowflake',
    sql='queries/create_temp_customers_table.sql',
    params={
      'batch_size': BATCH_SIZE
    },
    split_statements=True,
    return_last=True,
    handler=fetch_single_result
  )
  
  retrieve_batches = SQLExecuteQueryOperator.partial(
    task_id='customers_to_process_batch', 
    conn_id='snowflake', 
    sql='queries/customer_batch.sql',
    handler=fetch_single_result
  ).expand(params=[{'batch_size': BATCH_SIZE, 'batch_number': index} for index in range(int(f'{create_temporary_table}'))])
  

  #process_batch = processBatch(customers_to_process)

  #delete_temporary_table = SQLExecuteQueryOperator(
  #  task_id='delete_temporary_table', 
  #  conn_id='snowflake',
  #  sql='DROP TABLE brine.{{ params.temp_table_prefix }}_temp;',
  #  split_statements=True,
  #  return_last=True
  #)
  

  # # 3. Get erichs 
  # # TODO: Handle XCom Parsing 
  # erichs = SQLExecuteQueryOperator(
  #     task_id="get_erichs", 
  #     conn_id="snowflake", 
  #     sql="queries/experian_erichs.sql",
  # )
  create_temporary_table #customers_to_process, delete_temporary_table)

experianExtraction()
