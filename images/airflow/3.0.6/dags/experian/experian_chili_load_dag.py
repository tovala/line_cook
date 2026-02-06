from pendulum import duration

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Param
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.chili_load_task_group import loadIntoChili
@dag(
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=CronTriggerTimetable('0 3 * * *', timezone='America/Chicago'),
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration'],
  params={
    'channel_name': getSlackChannelNameParam(),
    'batch_size': Param(100, type='integer', minimum=1, maximum=300, description='number of customers per batch. Due to Experian API limitations, must be <= 300.'),
    'full_refresh': Param(False, type='boolean')
  }
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
    copy_table_args={
      'table': 'CHILI_V2.EXPERIAN_CUSTOMERS',
      'pattern': 'parsed_responses.*[.]json'
    } 
  )