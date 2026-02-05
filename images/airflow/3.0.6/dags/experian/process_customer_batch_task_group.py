import json
import re
import requests
from requests import HTTPError
from typing import Dict

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
    GET_TOKEN_URL = 'https://us-api.experian.com/oauth2/v1/token'
    
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
    
    return token_response_json['access_token']
  
  # TODO: this task should have an execution timeout of 30mins
  @task()
  def fetchFromExperian(erich_values: str, access_token: str, customer_batch: Dict[str, str]):
    '''
    Docstring for fetchFromExperian
    '''
    BATCH_URL = 'https://us-api.experian.com/marketing-services/targeting/v1/ue-microbatch'
    try:
      experian_response = requests.post(
        url= BATCH_URL,
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {access_token}'},
        data = json.dumps({
          'partyid': Variable.get('experian_party_id'),
          'erich': erich_values,
          'delimiter': '|',
          'layout': 'fname|lname|addr1|addr2|city|state|zip|email|phone|lead_id',
          'carryInput': 'true',
          **customer_batch
        })
      )
      
      experian_response.raise_for_status()
      experian_response_json = experian_response.json()

    except HTTPError as e:
      raise AirflowException(f'Error from Experian API: {e}')
      
    # TODO: return what we care about
    return experian_response_json

  
  @task()
  def parseResponse(experian_response_json):
    response_keys = experian_response_json['csv_header'].split('|')
    customer_values_list = [v.split('|') for k, v in experian_response_json.items() if re.search("rec\\d{1,3}", k)]
    customer_json_list = []

    for customer_values in customer_values_list:
        res_dict = dict(zip(response_keys, customer_values))

        customer_json_list.append(json.dumps(res_dict))
    
    return customer_json_list

  # TODO: response from experian to S3
  # result_to_s3 = S3CreateObjectOperator()
  @task()
  def testBatch():
    test_batch = {"rec1":"Dennis|Pearson|86 Lugo Ln||Napa|CA|94559|dennis@pearsonfamily.info|7072912069|1942160",
                  "rec2":"Gary|Kucera||||||gkucera223@gmail.com||1942166",
                  "rec3":"Danielle|Frazier||||||danielle.e.frazier1998@gmail.com||1942190",
                  "rec4":"Valorie|Rodriguez|8407 Cotton Dr||Richmond|TX|77469|vjrodriguez71@gmail.com|2819355793|1942199",
                  "rec5":"Aj|Vourakis||||||ajturbo@gmail.com||1942208",
                  "rec6":"Shelitha|Scott|804 E Radbard St||Carson|CA|90746|shelithascott@gmail.com|3109015094|1942216",
                  "rec7":"Lauren|Kaye|1336 W Grand Ave|Apt 2|Chicago|IL|60642|lauren92kaye@gmail.com|2028308178|1942219",
                  "rec8":"Naila||780 Ivory Ln||Haverhill|FL|33415|montoyanaila0@gmail.com|5617792083|1942221",
                  "rec9":"Todd|||||||trhaitsuka@yahoo.com||1942228",
                  "rec10":"Harold|Henley|4659 S Drexel Blvd|Apt 315|Chicago|IL|60653|cj284056@gmail.com|7739936348|1942237",
                  "rec11":"Nancy|Piereth|10 Broad Ave||Riverhead|NY|11901|mtkcwdncr@aol.com|6312362046|1942239",
                  "rec12":"Suann|Lanton|3512 S Utah St||Arlington|VA|22206|dc24473820@gmail.com|8088958441|1942257"}

    return test_batch  
  
  experian_token = getExperianToken()
  retrieved_batch = testBatch()
  #retrieved_batch = SQLExecuteQueryOperator(
  #  task_id=f'customers_to_process_batch', 
  #  conn_id='snowflake', 
  #  sql='queries/customer_batch.sql',
  #  handler=fetch_single_result
  #)

  fetch_from_experian = fetchFromExperian(erichs, experian_token, retrieved_batch)
  parsed_responses = parseResponse(fetch_from_experian)