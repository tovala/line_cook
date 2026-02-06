
import datetime

import json
import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from typing import Any, Dict, List
from pendulum import duration


from airflow.sdk import dag, task, chain, Variable
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.providers.slack.notifications.slack import SlackNotifier
from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from user_deletes.process_delete_requests_task_group import processDeleteRequests
from airflow.timetables.trigger import CronTriggerTimetable
from common.sql_operator_handlers import fetch_results_array


@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=CronTriggerTimetable('45 7 * * *', timezone='America/Chicago'),
    catchup=False,
    default_args={
       'retries': 2,
        'retry_delay': duration(seconds=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': duration(minutes=5),
    },
    tags=['internal', 'cleanup'],
    params={
        "channel_name": getSlackChannelNameParam()
    }
)
def user_deletes():
  '''User Deletes
  Description: Manage deleting users when requested from
    - CombinedAPI
    - CustomerIO
    - Airtable
    - Typeform
    - Any Outstanding Zendesk Tickets

  Schedule: Daily

  Dependencies:

  Variables:

  '''
  

  @task(
    on_failure_callback=SlackNotifier(
        slack_conn_id='tovala_slack',
        text='Snowflake Connection Failure for getUserExceptionIds',
        channel='team-data-notifications'
    ) 
  )
  def getUserExceptionIds() -> List[str]:
    '''
    Retrieve user IDs from Snowflake from a list of users flagged not to delete.
    
    Args:
    None

    Output:
    user_exception_ids (List[str]): list of the user exception ids returned from the query
    '''
    user_exception_ids = []
    query = '''SELECT zendesk_user_id
            FROM masala.brine.user_deletes_exceptions
            ORDER BY zendesk_user_id;'''

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake')
    sf_connection = sf_hook.get_conn()

    cursor = sf_connection.cursor()

    for user_ids in cursor.execute(query):
      user_exception_ids.append(user_ids)
    
    return user_exception_ids


  @task()
  def getDeleteRequests() -> List[Dict[str, Any]]:
    '''Retrieve all open Zendesk tickets with a `delete_request` tag with an HTTP Request to Zendesk Search API.
    See docs here: https://developer.zendesk.com/documentation/api-basics/working-with-data/searching-with-the-zendesk-api/

    Args:
    None

    Output:
    response_json (Dict[str, Any]): json of the response body, returns empty list if errors occur
    '''
    response_jsons = []
    url = 'https://tovala.zendesk.com/api/v2/search/export'
    params = {
    'query': 'tags:delete_request status:hold',
    'filter[type]': 'ticket'
    }
    auth = HTTPBasicAuth(Variable.get('zendesk_api_username'), Variable.get('zendesk_api_key'))
    headers = {'Content-Type': 'application/json'}

    # From here: https://developer.zendesk.com/documentation/api-basics/pagination/paginating-through-lists-using-cursor-pagination/#python-example-with-the-after-cursor
    try:
        while url:
            # Make the GET request with authentication and params
            print(user_deletes)
            response = requests.get(url, auth=auth, params=params)
            
            # Handle HTTP 429 Too Many Requests (rate limiting)
            if response.status_code == 429:
                retry_after = int(response.headers['retry-after'])
                print(f'Rate limited. Retrying after {retry_after} seconds...')
                datetime.time.sleep(retry_after)
                # Retry the request after waiting
                response = requests.get(url, auth=auth, params=params)

            response.raise_for_status()  # Raise exception on HTTP errors
            data = response.json()       # Parse JSON response

            # Append current page's users to the aggregate list
            response_jsons.extend(data['results'])
            
            # Check pagination metadata to see if more pages are available

            if data['meta']['has_more']: 
               url = data['links']['next']
              #  response = requests.get(url, auth=AUTH)
            else:
                url = None  # No more pages, exit loop

    except HTTPError as e:
        raise AirflowException(f'HTTP error occurred: {e}')
    except RequestException as e:
        raise AirflowException(f'Request error occurred: {e}')

    return response_jsons
      
  @task.short_circuit
  def nonEmptyDeleteRequests(zendesk_response: List[Dict[str, Any]]) -> bool:
     '''Short Circuit task that skips the User Deletes DAG if there are no open delete request tickets in the Zendesk response.

     Args:
      zendesk_response (List[Dict[str, Any]]): List of result objects from the Zendesk seach API call

     Output:
      (bool): True if there are active delete requests to process, otherwise false

     '''
     if not zendesk_response:
        return False
     else:
        return True


  @task()
  def getCombinedAPIToken() -> str:
    '''
    Get API Token from CAPI.

    Args:
      None

    Output:
      token (str): API token to make calls to CAPI endpoints.
    '''
    try:
      token_response = requests.post(
        url='https://api.tovala.com/v0/getToken',
        data=json.dumps({
          'type': 'user',
          'email': Variable.get('tovala_api_username'),
          'password': Variable.get('tovala_api_password'),
        }),
        headers={
          'Content-Type': 'application/json',
          'X-Tovala-AppID': 'user-delete-request',
        }
      )

      # Raise Error if response status != 200
      token_response.raise_for_status()
      token_response_json = token_response.json()

    except HTTPError as e:
      raise AirflowException(f'HTTP error occurred (getCombinedAPIToken): {e}')
    except RequestException as e:
        raise AirflowException(f'Request error occurred (getCombinedAPIToken): {e}')

    return token_response_json['token']

  @task()
  def parseDeleteRequests(response_json: Dict[str, Any], user_exception_ids: List[str]) -> List[Dict[str, Any]]:
    '''
    Parse the description string from the Zendesk ticket search results to get UserID, Email, and Commitment Status.

    Args:
    response_json (Dict[str, Any]): json of the Zendesk Search HTTP response

    Output:
    valid_delete_requests (List[Dict[str, str]]): A list of delete_request objects with key value pairs for UserID, Email, and Commitment
    e.g.  [{
            'UserID': 0000000,
            'Email': 'customer@tovala.com',
            'Commitment': True,
            'zendesk_ticket_id': 'zendeskticketid12234',
            'zendesk_requester_id': 'zendeskrequesterid1234'
          },
          {
            'UserID': 0000001,
            'Email': 'customer@gmail.com',
            'Commitment: False,
            'zendesk_ticket_id': 'zendeskticketid5678',
            'zendesk_requester_id': 'zendeskrequesterid5678'
          },
          ]
    '''
    valid_delete_requests = []

    exception_fail = []
    commitment_fail = []
    bad_request_fail = []
    
    for current_ticket in response_json:
      current_user_data = cleanTicketDescription(current_ticket.get('description'))

      current_user_id = current_user_data.get('UserID')
      on_commitment = current_user_data.get('Commitment')

      # If ticket discription was improperly formatted/unparsable, skip the record
      if not current_user_data:
         bad_request_fail.append(current_user_id)
         continue
      
      # User Exception Check
      if current_user_id in user_exception_ids:
        exception_fail.append(current_user_id)
        continue

      # Commitment Check
      if on_commitment:
        commitment_fail.append(current_user_id)
        continue

      # Add info about Zendesk ticket to user data
      current_user_data.update({
        'zendesk_ticket_id': current_ticket['id'],
        'zendesk_requester_id': current_ticket['requester_id'],
      })

      # Add user_data to the delete_requests list
      valid_delete_requests.append(current_user_data)
    
    return valid_delete_requests

  @task.short_circuit
  def nonEmptyParsedDeleteRequests(parsed_delete_requests_list: List[Dict[str, Any]]) -> bool:
     '''Short Circuit task that skips the User Deletes DAG if there are no valid, parsable delete requests.

     Args:
      parsed_delete_requests_list (List[Dict[str, Any]]): List of delete request objects from parsing step.

     Output:
      (bool): True if there are parsed delete requests to process, otherwise false

     '''
     if not parsed_delete_requests_list:
        return False
     else:
        return True

  @task(
    on_failure_callback=SlackNotifier(
        slack_conn_id='tovala_slack',
        text='Snowflake Connection Failure for getTypeformResponseIds',
        channel='team-data-notifications'
    )
  )
  def getTypeformResponseIds(parsed_delete_requests: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[str]]]:
    '''
    Retrieve forms with response IDs for each of the users with a delete request. Meant to run once for all delete requests.
    
    Args:
      delete_requests (List[Dict[str, Any]]): The complete list of parsed delete requests to be processed

    Output:
      forms_and_responses (Dict[str, Dict[str, List[str]]]): Dictionary with user ID (or email) as the primary key, with values of a list of dictionaries with form ID as key and a list of response IDs for that form as values
      e.g.
      {
        '1122334':{
            'formA': ['response1', 'response2'],
            'formB': ['response10']
        },
        '1234567':{
            'formA': ['response4', 'response98'],
            'formC': ['response323123']
        },
        'example@email.com':{
            'formA': ['response999'],
            'formC': ['response323123'],
            'formE': ['123', '3456']
        }
      }
    '''
    forms_and_responses = {}
    # For some unholy reason, we store userid in typeform_landings as a string
    user_ids = ', '.join(["'" + str(d['UserID']) + "'" for d in parsed_delete_requests])
    emails = ', '.join(["'" + d['Email'] + "'" for d in parsed_delete_requests])

    query = f'''WITH all_responses AS (
                  (SELECT 
                    form_id
                    , response_id
                    , userid 
                    , email
                  FROM dry.typeform_landings 
                  WHERE userid IN ({user_ids}))
                  UNION 
                  (SELECT 
                    form_id
                    , response_id
                    , userid 
                    , email
                  FROM dry.typeform_landings 
                  WHERE email IN ({emails})))
                SELECT 
                  CASE WHEN userid IS NOT NULL
                       THEN userid 
                       ELSE LOWER(email) 
                  END AS user_identifier
                  , form_id
                  , ARRAY_TO_STRING(ARRAY_AGG(response_id), ', ')
                FROM all_responses 
                GROUP BY 1,2;'''

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake')
    sf_connection = sf_hook.get_conn()

    cursor = sf_connection.cursor()

    for response in cursor.execute(query):
      form_dict = {}
      form_dict[response[1]] = response[2].split(', ')
      forms_and_responses[response[0]] = form_dict
    
    return forms_and_responses


  @task()
  def completeDeleteRequests(parsed_delete_requests: List[Dict[str, Any]], typeform_data: Dict[str, Dict[str, List[str]]]) -> List[Dict[str, Any]]:
    '''Add necessary data queried from Snowflake to the user delete requests for processing.

    Args:
      parsed_delete_requests (List[Dict[..]]): List of delete request objects returned by parseDeleteRequests task.
      typeform_data (Dict[str, Dict[str, List[str]]]): Dict with user ID (or email if no user ID available) keys and Dict values with form IDs mapped to associated response IDs for the given user.
    
    Output:
      complete_delete_requests (List[Dict[str, Any]]): List of delete request objects containing all data required to process a delete for the given user.
    '''
    complete_delete_requests = []
    
    for pdr in parsed_delete_requests:
      user_id = str(pdr.get('UserID'))
      email = pdr.get('Email').lower()

      # Get any entries in the typeform data 
      typeform_data_user_id = typeform_data.get(user_id, {})
      typeform_data_email = typeform_data.get(email, {})
      
      # TODO: handle edge case - same key in userid and email
      all_typeform_user_data = typeform_data_user_id | typeform_data_email

      pdr['typeform_data'] = all_typeform_user_data
      complete_delete_requests.append(pdr)

    return complete_delete_requests

  # DAG Implementation
  
  # pulls all active delete requests from Zendesk
  delete_requests_json = getDeleteRequests()
  
  # pre-processing steps
  user_exception_ids = SQLExecuteQueryOperator(
    task_id='user_exception_ids', 
    conn_id='snowflake', 
    sql='queries/user_exception_ids.sql',
    handler=fetch_results_array,
  )

  parsed_delete_requests = parseDeleteRequests(delete_requests_json, user_exception_ids.output)
  typeform_data = getTypeformResponseIds(parsed_delete_requests)

  complete_delete_requests = completeDeleteRequests(parsed_delete_requests, typeform_data)

  # If there are no delete requests, short-circuit before pre-processing
  chain(nonEmptyDeleteRequests(delete_requests_json), user_exception_ids, parsed_delete_requests)
  # If none of the delete requests can be parsed, short-circuit
  chain(nonEmptyParsedDeleteRequests(parsed_delete_requests), typeform_data, complete_delete_requests)
  
  # If there are valid delete requests to process, get CAPI token and process each ticket individually
  capi_token = getCombinedAPIToken()
  processDeleteRequests.partial(capi_token=capi_token).expand(delete_request=complete_delete_requests)



# DAG call
user_deletes()







# Helper functions that are not tasks
def cleanTicketDescription(ticket_desc: str) -> Dict[str, Any]:
  '''
  Take the string value from the Zendesk ticket that contains User info and convert to a dict of key/value pairs. Fixes typing.
  If unable to parse the ticket description, returns an empty Dict.
  
  Args:
  ticket_desc (str): a str representation of the user data from the Zendesk ticket description. 
  e.g. 'UserID: 0000000\nEmail: customer@gmail.com\nCommitment: false'
  
  Output:
  user_data_dict (Dict[str, Any]): Dict representation of the user data from the Zendesk ticket description.
  {
    'UserID': 0000000,
    'Email': 'customer@gmail.com',
    'Commitment': True
  }

  '''
  user_data_dict = {}

  try:
    desc_list = ticket_desc.split('\n') 

    for item in desc_list:
      # Split each of the elements in the desc list, like 'UserID: 0000000' into key = 'UserID' and value = '0000000'
      key, value = item.split(': ')
      # convert str boolean values to Bool
      if value.lower() == 'true':
        user_data_dict[key] = True
        continue
      elif value.lower() == 'false':
        user_data_dict[key] = False
        continue

      # convert str to int if possible
      try:
        user_data_dict[key] = int(value)
      except ValueError:
        user_data_dict[key] = value

    return user_data_dict
  except ValueError:
     return {}