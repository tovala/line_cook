
import json
import requests
from pyairtable import Api as AirtableApi
from pyairtable import formulas
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from typing import Any, Dict


from airflow.sdk import task, task_group, Variable, chain
from airflow.exceptions import AirflowException

@task_group(group_id='process_delete_requests')
def processDeleteRequests(capi_token:str, delete_request:Dict[str, Any]) -> None:
  '''Series of individual delete tasks for each service that needs data removed that run together per individual user delete request.
  
  Runs deleteUserCombinedAPI, deleteUserCustomerIO, deleteUserAirtable, deleteUserTypeform in parallel.
  Runs updateZendeskTicketToClosed and deleteUserZendesk only if all of the initial delete request tasks were successful.

  Args:
    capi_token (str): API token to make calls to CAPI db
    delete_request (Dict[str, Any]): A delete request object for a single user
    e.g.
    {
      'Email': 'example@gmail.com',
      'UserID': 1234567, 
      'Commitment': False,
      'typeform_data':{
                        'GBOKHQ': ['responseid45tr3784gfh3498', 'responseidrgw443rwfw4433'],
                        'QWERTY': ['responseid13412342352wefwef3e4f']
                      },
      'zendesk_ticket_id': 1111112,
      'zendesk_requester_id': 11111122222233
    }
  
  Output:
    None
  '''
  
  @task()
  def deleteUserCombinedAPI(token: str, delete_request: Dict[str, Any]) -> bool:
    '''Make a request to CAPI to delete a user by ID from the db.
    CAPI will return:
    404 - Request valid, user not found
    400 - Request correctly formed, invalid (non-int) UserID passed
    500 - DB error, can retry

    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
      (bool) - True, unless exception is raised.
    '''
    try:
      userID = delete_request['UserID']
      delete_user_response = requests.put(
        url= f'https://api.tovala.com/v1/users/{userID}/deleteUserData',
        headers={
          'Content-Type': 'application/json',
          'X-Tovala-AppID': 'airflow',
          'Authorization': f'Bearer {token}'
        }
      )
      
      if delete_user_response.status_code == 404:
        # request was correctly formatted, but User did not exist in CAPI
        # we should still continue and try to delete the user from all other locations
        return True
      
      delete_user_response.raise_for_status()

    except HTTPError as e:
      raise AirflowException(f'HTTP error occurred (deleteUserCombinedAPI): {e}')
    except RequestException as e:
      raise AirflowException(f'Request error occurred (deleteUserCombinedAPI): {e}')
    
    return True
    
  
  @task()
  def deleteUserCustomerIO(delete_request: Dict[str, Any]) -> bool:
    '''Make request to CustomerIO API to delete user from system based on their email.
    
    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
      (bool) - True, unless exception is raised.
    '''
    email = delete_request['Email']
    cio_auth = HTTPBasicAuth(Variable.get('cio_site_id'), Variable.get('cio_api_key'))

    cio_response = requests.post(
        url='https://track.customer.io/api/v2/entity',
        auth=cio_auth,
        headers={
          'Content-Type': 'application/json',
        },
        data=json.dumps({
          'type': 'person',
          'identifiers': {
              'email': email
          },
          'action': 'delete'
        })
      )
    
    try:
      cio_response.raise_for_status()
    
    except HTTPError as e:
      cio_response_json = cio_response.json()
      raise AirflowException(f'HTTP error occurred (deleteUserCustomerIO): {e} - {cio_response_json['errors']}')
    except RequestException as e:
      raise AirflowException(f'Request error occurred (deleteUserCustomerIO): {e}')
    
    return True

  
  @task()
  def deleteUserAirtable(delete_request: Dict[str, Any]) -> bool:
    '''Delete any data from menu and marketplace feedback airtable tables associated with the user in the delete request.

    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
        (bool) - True, unless exception is raised.
    '''
    userID = delete_request['UserID']
    airtable_api = AirtableApi(Variable.get('airtable_auth_token'))
    tables_and_columns = {'menu_feedback': 'userID', 'marketplace_feedback': 'User ID'}
    
    
    for table_id, search_key in tables_and_columns.items():
      table = airtable_api.table(Variable.get('airtable_base_id'), table_id)
      records_to_delete = []

      # retrieve feedback records associated with the given user 
      all_records = table.all(formula=formulas.match({search_key: userID}))      

      # Only attempt delete if user has a record of this type
      if all_records:
        for record in all_records:
          records_to_delete.append(record['id'])

        # delete all records for this user
        try:
          batch_delete_responses = table.batch_delete(records_to_delete)
          batch_delete_failures = [response['id'] for response in batch_delete_responses if not response['deleted']]
        except HTTPError as e:
          raise AirflowException(f'HTTP error occurred (deleteUserAirtable, table_id: {table_id}): {e}')

        # if any responses were not deleted successfully, raise Error
        if batch_delete_failures:
          raise AirflowException(f'Error deleting responses from {table_id} table for User ID {userID}')
    
    return True


  @task()
  def deleteUserTypeform(delete_request: Dict[str, Any]) -> bool:
    '''Make request to Typeform API to delete any responses a user left on typeform forms (as associated by userId and/or email).
    Typeform data in delete request comes from getTypeformResponseIds task.

    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
      (bool) - True, unless exception is raised.
    '''
    typeform_data = delete_request.get('typeform_data')

    for form_id, response_ids in typeform_data.items():
      typeform_response = requests.delete(
        url=f'https://api.typeform.com/forms/{form_id}/responses',
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {Variable.get('typeform_key')}'
        },
        data=json.dumps({'included_response_ids': response_ids})
      )
      
      try:
        typeform_response.raise_for_status()

      except HTTPError as e:
        typeform_response_json = typeform_response.json()
        raise AirflowException(f'HTTP error occurred (deleteUserTypeform): {e} - {typeform_response_json['description']}')
      except RequestException as e:
        raise AirflowException(f'Request error occurred (deleteUserTypeform): {e}')
      
      return True


  @task()
  def updateZendeskTicketToClosed(delete_request: Dict[str, Any]) -> Dict[str, Any]:
    '''Make request to Zendesk Ticketing API to update the status of the current delete request ticket to closed.

    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
      delete_request (Dict[str, Any]): Returns the delete request object containing details of the ticket that was just closed
    '''
    ticket_id = delete_request['zendesk_ticket_id']

    try:
      update_ticket_response = requests.put(
        url= f'https://tovala.zendesk.com/api/v2/tickets/{ticket_id}',
        headers={
            'Content-Type': 'application/json',
        },
        auth= HTTPBasicAuth(Variable.get('zendesk_api_username'), Variable.get('zendesk_api_key')),
        data=json.dumps({
                    'ticket': {
                    'status': 'closed'
                    }
                })
      )

      update_ticket_response.raise_for_status()
    
    except HTTPError as e:
      raise AirflowException(f'HTTP error occurred (updateZendeskTicketToClosed): {e}')
    except RequestException as e:
      raise AirflowException(f'Request error occurred (updateZendeskTicketToClosed): {e}')

    return delete_request
  

  @task_group(group_id='delete_user_zendesk')
  def deleteUserZendesk(zendesk_requester_id: str) -> None:
    '''Delete User from Zendesk.
    In order to permanently delete the user, you must make 2 calls (see docs: https://developer.zendesk.com/api-reference/ticketing/users/users/#permanently-delete-user)
    
    Args:
      delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

    Output:
      None
    '''
    
    @task()
    def softDelete(zendesk_requester_id: str) -> bool:
      '''Soft Delete Zendesk API Call.

      Args:
        delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

      Output:
        (bool) - True, unless exception is raised.
      '''
      delete_header = {
        'Content-Type': 'application/json',
      }
      delete_auth = HTTPBasicAuth(Variable.get('zendesk_api_username'), Variable.get('zendesk_api_key'))
      
      try:
        initial_delete_response = requests.delete(
          url = f'https://tovala.zendesk.com/api/v2/users/{zendesk_requester_id}',
          headers = delete_header,
          auth= delete_auth
        )

        if initial_delete_response.status_code == 422:
          # if Zendesk returns a 422, this indicates that the request was correctly formed but there is logic on the Zendesk side that prevents the user from being deleted (does not exist, incompatible status, etc.)
          # this is outside engineering's scope, and not something we can fix, nor does it indicate a true error or bug, so we skip these when deleting the user from Zendesk
          return True
        
        initial_delete_response.raise_for_status()
      
      except HTTPError as e:
        raise AirflowException(f'HTTP error occurred (deleteUserZendesk - soft delete): {e}')
      except RequestException as e:
        raise AirflowException(f'Request error occurred (deleteUserZendesk - soft delete): {e}')
      
      return True

    @task()
    def permanentDelete(zendesk_requester_id: str) -> bool:
      '''Permanent Delete Zendesk API Call. Required for legal compliance.

      Args:
        delete_request (Dict[str, Any]): A delete request object for a single user (See processDeleteRequests task_group docstring for schema)

      Output:
        (bool) - True, unless exception is raised.
      '''
      delete_header = {
        'Content-Type': 'application/json',
      }
      delete_auth = HTTPBasicAuth(Variable.get('zendesk_api_username'), Variable.get('zendesk_api_key'))
      
      try:
        permanent_delete_response = requests.delete(
          url = f'https://tovala.zendesk.com/api/v2/deleted_users/{zendesk_requester_id}',
          headers = delete_header,
          auth = delete_auth
        )

        if permanent_delete_response.status_code == 422:
          # if Zendesk returns a 422, this indicates that the request was correctly formed but there is logic on the Zendesk side that prevents the user from being deleted (does not exist, incompatible status, etc.)
          # this is outside engineering's scope, and not something we can fix, nor does it indicate a true error or bug, so we skip these when deleting the user from Zendesk
          return True
        if permanent_delete_response.status_code == 404:
          # if Zendesk returns a 404, this indicates that the request was correctly formed but there's not a deleted user with that id, so cannot be permanently deleted
          # The user already doesn't exist, and we've now made a good faith effort to remove the user from all systems, so this shouldn't return an error/
          return True
        
        permanent_delete_response.raise_for_status()

      except HTTPError as e:
        raise AirflowException(f'HTTP error occurred (deleteUserZendesk - permanent delete): {e}')
      except RequestException as e:
        raise AirflowException(f'Request error occurred (deleteUserZendesk - permanent delete): {e}')
      
      return True
    
    chain(softDelete(zendesk_requester_id), permanentDelete(zendesk_requester_id))

  # process_user_deletes task group implementation
  capi_delete = deleteUserCombinedAPI(capi_token, delete_request)
  airtable_delete = deleteUserAirtable(delete_request)
  cio_delete = deleteUserCustomerIO(delete_request)
  typeform_delete = deleteUserTypeform(delete_request)

  closed_ticket = updateZendeskTicketToClosed(delete_request)
  zendesk_delete = deleteUserZendesk(closed_ticket['zendesk_requester_id'])

  chain([capi_delete, airtable_delete, cio_delete, typeform_delete], closed_ticket, zendesk_delete)