from datetime import datetime
from airflow.timetables.trigger import CronTriggerTimetable

import json
import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError, RequestException
from pendulum import duration

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from common.slack_notifications import bad_boy, good_boy

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
    @task()
    def getExperianToken():
        """
        :return: authorization token for requests, valid for 30 minutes; None if response not returned
        """
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
        except:
            # TODO: Handle Errors
            print('Oh shit!')

        return token_response.json()['refresh_token']

    @task()
    #TODO: handle full refresh flag - set as parameter? 
    # Currently just including this to have the code in here 
    def fetch_experian_data(full_refresh=False):
        """ Driver function for fetching missing customers and processing them in batches. 
        :param batch_size: Number of customers to process at once. Max is 300. 
        """
        pass
        # 1. Get token 
        # auth_token = get_token(GET_TOKEN_URL)
        # token_time = datetime.now()

        # # 2. Get customers and erich 
        # erichs = get_erich_fields()
        # all_customers = get_customers(full_refresh)
        # failed_customers = []

        # # 3. Batch and process customers 
        # while len(all_customers) > 0:
        #     if (datetime.now() - token_time).seconds/60 > 28:
        #         auth_token = get_token(GET_TOKEN_URL)
        #     customer_batch = all_customers[0:batch_size]
        #     customer_batch = [['rec' + str(customer_batch.index(item)+1)] + item for item in customer_batch]
        #     customer_batch_dict = {item[0]: item[1] for item in customer_batch}
        #     request_body = generate_batch_request_body(customer_batch_dict, erichs)
        #     success_flag = process_batch(request_body, auth_token)
        #     if not success_flag:
        #         failed_customers += customer_batch
        #     del all_customers[0:batch_size]
        #     print(F"{len(all_customers)} remaining!")

        # if len(failed_customers) > 0:
        #     print("Failed to load the following customers from Experian:")
        #     print(failed_customers)
        #     cap.publish_to_sns(F"Failed to load {len(failed_customers)} customers from Experian.", F"Experian Failure", C.SNS_PATH)

    # 1. Get Token 
    # TODO: Figure out how to regenerate token after 30 minutes 
    access_token = getExperianToken()    

    # 2. Get list of customers -- how to batch this in an Airflow-y way? 
    # TODO: Handle XCom Parsing - each record looks like: {'__data__': ['Samantha|Stuart|4845 SE Vintage Pl||Milwaukie|OR|97267|samanthastuart_dc@yahoo.com|5033208542|1936108'], '__version__': 1, '__classname__': 'builtins.tuple'}
    customers = SQLExecuteQueryOperator(
        task_id="customers_to_process", 
        conn_id="snowflake", 
        sql="queries/experian_customers.sql",
    )

    # 3. Get erichs 
    # TODO: Handle XCom Parsing 
    erichs = SQLExecuteQueryOperator(
        task_id="get_erichs", 
        conn_id="snowflake", 
        sql="queries/experian_erichs.sql",
    )

    # 4. For each batch:
        # Get Data
        # Yeet to S3 - how to do without writing file to system? 
    # 5. Load data from S3 into Snowflake 
    # TODO: Set up bucket/stage in sous_chef/move over existing files
        # Will need to repoint tables in spice_rack to look at chili_v2.experian_customers and remove existing chili table

fetchExperianData()

