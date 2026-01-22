from datetime import datetime

import json
import re
import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from pendulum import duration

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowException
from common.slack_notifications import bad_boy, good_boy

# GET_TOKEN_URL = 'https://us-api.experian.com/oauth2/v1/token'
NEW_PASSWORD_URL = 'https://ss3.experian.com/securecontrol/reset/passwordreset?command=requestnewpassword&application=netconnect&version=1'
# UPDATE_PASSWORD_URL = "https://ss3.experian.com/securecontrol/reset/passwordreset?newpassword=%s&command=resetpassword&application=netconnect&version=1"
# BATCH_URL = "https://us-api.experian.com/marketing-services/targeting/v1/ue-microbatch"

# EXPERIAN_USERNAME_PATH = '/spice_rack/snowflake/experian_username'
# EXPERIAN_PASSWORD_PATH = '/spice_rack/snowflake/experian_pw'
# EXPERIAN_CLIENT_ID_PATH = '/spice_rack/snowflake/experian_client_id'
# EXPERIAN_CLIENT_SECRET_PATH = '/spice_rack/snowflake/experian_client_secret'
# EXPERIAN_PARTY_ID_PATH = '/spice_rack/snowflake/experian_party_id'
# EXPERIAN_BATCH_LAYOUT = 'fname|lname|addr1|addr2|city|state|zip|email|phone|lead_id'
# EXPERIAN_FILEBASE = 'experian/'

@dag(
    #on_failure_callback=bad_boy,
    #on_success_callback=good_boy,
    # schedule=duration(days=28),
    catchup=False,
    # default_args={
    #     'retries': 2,
    #     'retry_delay': duration(seconds=2),
    #     'retry_exponential_backoff': True,
    #     'max_retry_delay': duration(minutes=5),
    # },
    tags=['internal', 'cleanup'],
    params={
        'channel_name': '#team-data-notifications'
    }
)
def monthlyExperianPasswordUpdate():
  '''Monthly Experian Password Update
  Description: Experian requires password to be updated every 30 days. This DAG automates that process every 28 days,
  so in the event of a failure we will not be locked out of the account.

  Schedule: Every 28 days

  Dependencies:

  Variables:

  '''
  @task()
  def fetchNewPassword():
      """Fetches and parses suggested pw from Experian API
      :param new_pw_url: URL to generate new password from Experian
      :return: new password suggestion (if successfully generated), None otherwise
      """      
      try:
        password_html = requests.post(
          url= NEW_PASSWORD_URL,
          headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
          },
          auth=HTTPBasicAuth(Variable.get('experian_username'), Variable.get('experian_password')),
        )

        # Experian returns this as plain text for some unholy reason
        new_password = re.findall("^<Response><newPassword>(.*)</newPassword></Response>$", password_html.text)
        assert len(new_password) == 1
      except:
        raise AirflowException('No password returned.')

      return new_password[0]

  @task()
  def update_saved_password(set_pw_url, new_password):
      """Sets saved password in Experian
      :param set_pw_url: URL to update password in Experian
      :param new_password: new password
      :return: a dictionary containing the new password and the time it was reset (if successful), None o/w
      """
      parsed_url =  set_pw_url % new_password
      headers = {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Cache-Control': 'no-cache',
          'Connection': 'keep-alive'
      }
      un = cap.get_parameter_store(EXPERIAN_USERNAME_PATH)
      pw = get_experian_pw()
      authorization = HTTPBasicAuth(un, pw)
      password_update_response = cap.send_request("POST", parsed_url, data={}, headers=headers, auth=authorization)
      if not password_update_response:
          # Update has failed
          return None 
      reset_time = datetime.now()
      new_password_dict = {"reset_time":reset_time, "password":new_password}
      return new_password_dict

  # 1. Retrieve the new password
  new_password = fetchNewPassword()
  
monthlyExperianPasswordUpdate()