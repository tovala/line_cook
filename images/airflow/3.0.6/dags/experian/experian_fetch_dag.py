import json
import math
import requests
from requests import HTTPError
from typing import List, Dict, Any

from airflow.sdk import dag, task, task_group, Variable, chain
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

BATCH_SIZE = 300
GET_TOKEN_URL = 'https://us-api.experian.com/oauth2/v1/token'

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
    'channel_name': '#team-data-notifications'
  }
)
def fetchExperianData():
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
  @task_group()
  def processBatch(customer_batch: str):
    '''
    Fetches experian data for a single batch of 300 customers.

    Args:
      customer_batch (str): formatted string of customer data to pass to Experian API
    '''

    #TODO: pull next batch from temp table cursor - can happen in parallel w get token
    
    @task()
    def getExperianToken():
      '''
      :return: authorization token for requests, valid for 30 minutes; None if response not returned
      '''
      try:
        auth_values = {
          'username': Variable.get('experian_username'),
          'password': Variable.get('experian_password'),
          'client_id': Variable.get('experian_client_id'),
          'client_secret': Variable.get('experian_client_secret')
        }
        token_response = requests.post(
          url= GET_TOKEN_URL,
          headers = {'Content-Type': 'application/json'},
          data = json.dumps(auth_values)
        )
        token_response.raise_for_status()
        token_response_json = token_response.json()

      except HTTPError as e:
        raise AirflowException()
      
      return token_response_json['refresh_token']
        
    # TODO: this task should have an execution timeout of 30mins
    @task()
    def fetchFromExperian(access_token, customer_batch: Dict[str, str]):
      '''
      Docstring for fetchFromExperian
      '''
      try:
        experian_response = requests.post(
          url= GET_TOKEN_URL,
          headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'},
          data = json.dumps({
            'partyid': Variable.get('experian_party_id'),
            'delimiter': '|',
            'layout': 'fname|lname|addr1|addr2|city|state|zip|email|phone|lead_id', **customer_batch})
          )
        
        experian_response.raise_for_status()
        experian_response_json = experian_response.json()

      except HTTPError as e:
        raise AirflowException()
        
      # TODO: return what we care about

    
    @task()
    def parseResponse(customer_batch, experian_response_json):
      # TODO: process results from experian API - match back to the lead_id, format for load to snowflake
      pass

    # TODO: response from experian to S3
    result_to_s3 = S3CreateObjectOperator()

    access_token = getExperianToken()

    fetch_from_experian = fetchFromExperian(access_token, customer_batch)

    chain(access_token, fetch_from_experian, result_to_s3)

  create_temporary_table = SQLExecuteQueryOperator(
    task_id="create_temporary_table", 
    conn_id="snowflake",
    sql="queries/create_temp_customers_table.sql",
    split_statements=True,
    return_last=True
  )
  
  for batch in range(math.ceil(create_temporary_table/BATCH_SIZE)):
    # TODO: return multiple instances of this task
    customers_to_process = SQLExecuteQueryOperator(
      task_id="customers_to_process", 
      conn_id="snowflake", 
      sql="queries/customer_batch.sql",
      params={
        'batch_size': BATCH_SIZE,
      }
    ).expand() # TODO: expand based on size of temp table

  process_batch = processBatch(customers_to_process)
  

  # # 3. Get erichs 
  # # TODO: Handle XCom Parsing 
  # erichs = SQLExecuteQueryOperator(
  #     task_id="get_erichs", 
  #     conn_id="snowflake", 
  #     sql="queries/experian_erichs.sql",
  # )
