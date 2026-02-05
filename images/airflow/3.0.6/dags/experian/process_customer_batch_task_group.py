import json
import re
import requests
from requests import HTTPError
from typing import Dict
from pendulum import duration

from airflow.sdk import task, task_group, Variable, chain
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def fetch_single_result(cursor):
  output, = cursor.fetchone()

  return output

@task_group(group_id='process_customer_batch')
def processBatch(erichs: str, stupid_list:Dict[str, str]):
  '''
  Fetches experian data for a single batch of 300 customers.

  Args:
    customer_batch (str): formatted string of customer data to pass to Experian API
  '''
  @task()
  def getExperianToken():
    '''
    :return: authorization token for requests, valid for 30 minutes; None if response not returned
    '''
    get_token_url = 'https://us-api.experian.com/oauth2/v1/token'
    
    try:
      auth_values = {
        'username': Variable.get('experian_username'),
        'password': Variable.get('experian_password'),
        'client_id': Variable.get('experian_client_id'),
        'client_secret': Variable.get('experian_client_secret')
      }
      token_response = requests.post(
        url= get_token_url,
        headers = {'Content-Type': 'application/json'},
        data = json.dumps(auth_values)
      )
      token_response.raise_for_status()
      token_response_json = token_response.json()

    except HTTPError as e:
      raise AirflowException()
    
    return token_response_json['access_token']
  
  # TODO: this task should have an execution timeout of 30mins
  @task(execution_timeout=duration(minutes=30))
  def fetchFromExperian(erich_values: str, access_token: str, customer_batch: str):
    '''
    Docstring for fetchFromExperian
    '''
    batch_url = 'https://us-api.experian.com/marketing-services/targeting/v1/ue-microbatch'
    customer_batch_dict = json.loads(customer_batch)
    
    try:
      experian_response = requests.post(
        url= batch_url,
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'},
        data = json.dumps({
          'partyid': Variable.get('experian_party_id'),
          'erich': erich_values,
          'delimiter': '|',
          'layout': 'fname|lname|addr1|addr2|city|state|zip|email|phone|lead_id',
          'carryInput': 'true',
          **customer_batch_dict
        })
      )
      
      experian_response.raise_for_status()
      experian_response_json = experian_response.json()

    except HTTPError as e:
      raise AirflowException(f'Error from Experian API: {e}')
      
    return experian_response_json

  
  @task()
  def parseResponse(experian_response_json):
    response_keys = experian_response_json['csv_header'].split('|')
    customer_values_list = [v.split('|') for k, v in experian_response_json.items() if re.search("rec\\d{1,3}", k)]
    customer_json_list = []

    for customer_values in customer_values_list:
        res_dict = dict(zip(response_keys, customer_values))

        customer_json_list.append(json.dumps(res_dict))
    
    # return a newline-delimited string of the json blobs
    return '\n'.join(customer_json_list) 
  
  experian_token = getExperianToken()

  retrieved_batch = SQLExecuteQueryOperator(
    task_id=f'customers_to_process_batch', 
    conn_id='snowflake', 
    sql='queries/customer_batch.sql',
    handler=fetch_single_result
  )

  fetch_from_experian = fetchFromExperian(erichs, experian_token, retrieved_batch.output)
  parsed_responses = parseResponse(fetch_from_experian)

  responses_to_s3 = S3CreateObjectOperator(
    task_id='experian_responses_to_s3',
    s3_bucket='tovala-data-experian',
    s3_key='parsed_responses_{{ run_id }}_batch{{ ti.map_index }}.txt',
    data=parsed_responses
  )