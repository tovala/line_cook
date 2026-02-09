import os
from pendulum import duration

from airflow.sdk import dag

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.chili_load_task_group import loadIntoChili

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
  dag_id='experian_load_to_chili',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=None, # Externally triggered by experian_extraction_dag
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    'channel_name': getSlackChannelNameParam()
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates'
)
def experianLoad():
  ''' Experian Extraction Pipeline
  Description: Retrieves Experian information for new customers

  Schedule: Daily at 3AM

  Dependencies:

  Variables:

  '''
  load_into_chili = loadIntoChili(
    file_type='JSON',
    stage_name='experian_stage',
    s3_url='s3://tovala-data-experian',
    sf_storage_integration='EXPERIAN_STORAGE_INTEGRATION',
    table='EXPERIAN_CUSTOMERS',
    columns_array=['raw_data', 'filename', 'updated', 'customer_id'],
    transform_columns='queries/copy_into_transform_columns.sql',
    pattern='parsed_responses.*[.]json'
  )

experianLoad()