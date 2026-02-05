from pendulum import duration

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, Param
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy
from experian.pre_process_setup_task_group import preProcessSetup
from experian.process_customer_batch_task_group import processBatch

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
    'channel_name': '',
    'temp_table_prefix': Param('experian_customers', type='string'),
    'batch_size': Param(12, type='integer', minimum=5)
  }
)
def experianExtraction():
  ''' Experian Extraction Pipeline
  Description: Retrieves Experian information for new customers

  Schedule: Daily at 3AM

  Dependencies:

  Variables:

  '''
  pre_process_setup = preProcessSetup()
  
  process_batch = processBatch.partial(erichs=pre_process_setup['erichs']).expand(stupid_list=pre_process_setup['stupid_list'])
  
  delete_temporary_table = SQLExecuteQueryOperator(
    task_id='delete_temporary_table', 
    conn_id='snowflake',
    sql='DROP TABLE brine.{{ params.temp_table_prefix }}_temp;'
  )
   
  process_batch >> delete_temporary_table

experianExtraction()
