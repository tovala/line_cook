from datetime import datetime
from zoneinfo import ZoneInfo

import json
import re
import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from pendulum import duration

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowException
from common.slack_notifications import bad_boy, good_boy

NEW_PASSWORD_URL = 'https://ss3.experian.com/securecontrol/reset/passwordreset?command=requestnewpassword&application=netconnect&version=1'
UPDATE_PASSWORD_URL = 'https://ss3.experian.com/securecontrol/reset/passwordreset?newpassword=%s&command=resetpassword&application=netconnect&version=1'

@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=duration(days=28),
    start_date=datetime(2026, 2, 1, 3, tzinfo=ZoneInfo('America/Chicago')),
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': duration(seconds=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': duration(minutes=5),
    },
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
      :return: new password suggestion
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
        new_password = re.findall('^<Response><newPassword>(.*)</newPassword></Response>$', password_html.text)
        assert len(new_password) == 1
      except:
        raise AirflowException('No password returned.')

      return new_password[0]

  @task()
  def updateSavedPassword(new_password):
      """Sets saved password in Experian
      :param new_password: new password
      :return: a dictionary containing the new password and the time it was reset (if successful), None o/w
      """
      parsed_url =  UPDATE_PASSWORD_URL % new_password
      print(parsed_url)

      try:
        password_update_response = requests.post(
          url= parsed_url,
          headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
          },
          auth=HTTPBasicAuth(Variable.get('experian_username'), Variable.get('experian_password')),
        )

        # Response is encoded in html
        response_code = re.findall('<responseCode>(.*)</responseCode>', password_update_response.text)[0]
        response_message = re.findall('<responseMessage>(.*)</responseMessage>', password_update_response.text)[0]
        
        assert response_code == '200'
      except:
        raise AirflowException(f'Password unable to be reset: {response_message}, {response_code}')

      return response_code, response_message

  @task()
  def setAirflowPassword(new_password):
    Variable.set('experian_password', new_password)

  # 1. Retrieve the new password
  suggested_password = fetchNewPassword()

  # 2. Set new password in Experian and #3. Update in Airflow
  updateSavedPassword(suggested_password) >> setAirflowPassword(suggested_password)

  
monthlyExperianPasswordUpdate()