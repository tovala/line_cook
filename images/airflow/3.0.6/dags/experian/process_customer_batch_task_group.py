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
  # result_to_s3 = S3CreateObjectOperator()

  access_token = getExperianToken()

  fetch_from_experian = fetchFromExperian(access_token, customer_batch)

  chain(access_token, fetch_from_experian, result_to_s3)